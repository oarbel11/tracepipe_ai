"""
╔══════════════════════════════════════════════════════════════════╗
║                  PEER REVIEW ORCHESTRATOR                        ║
╚══════════════════════════════════════════════════════════════════╝

Main orchestrator that synthesizes all validations into a commit advisory.

USAGE:
    from peer_review import PeerReviewOrchestrator
    
    orchestrator = PeerReviewOrchestrator()
    advisory = orchestrator.review_changes()
    
    print(advisory.formatted_output)
"""

import re
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, asdict
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.peer_review.semantic_delta import SemanticDeltaExtractor, SQLPatternMatcher
from scripts.peer_review.technical_validator import TechnicalValidator
from scripts.debug_engine import DebugEngine

# Suppress verbose logging — only show our clean output
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class CommitAdvisory:
    """Final commit advisory with all analysis results."""
    risk_level: str  # 'GREEN', 'YELLOW', 'RED'
    advisory: str  # 'Safe to commit', 'Proceed with caution', 'Manual review required'
    
    # Component results
    semantic_delta: Dict[str, Any]
    blast_radius: Dict[str, Any]
    technical_report: Dict[str, Any]
    business_report: Dict[str, Any]
    
    # Summary
    files_changed: int
    technical_blockers: int
    business_impact_summary: str
    
    # Formatted output
    formatted_output: str
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PEER REVIEW ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PeerReviewOrchestrator:
    """
    Main orchestrator for the Senior Peer Review system.
    
    Focused on two core checks:
    1. SQL syntax errors (typos, unmatched parens)
    2. Impact chain (which tables changed → which downstream tables are affected)
    """
    
    def __init__(
        self,
        repo_path: Optional[str] = None,
        gemini_api_key: Optional[str] = None
    ):
        # Initialize debug engine (for lineage lookups)
        try:
            self.debug_engine = DebugEngine()
        except Exception as e:
            logger.warning(f"Could not initialize DebugEngine: {e}")
            self.debug_engine = None
        
        self.delta_extractor = SemanticDeltaExtractor(repo_path)
        self.technical_validator = TechnicalValidator(self.debug_engine)

    def review_changes(self, staged_only: bool = False) -> CommitAdvisory:
        """
        Review code changes and generate commit advisory.
        
        Simplified flow:
        1. Get changed SQL from git diff
        2. Check syntax errors
        3. Find which tables changed (per-block diff)
        4. Trace downstream impact chain via lineage DB
        5. Format clean output
        """
        # Suppress all noisy loggers
        for name in ['databricks', 'scripts', 'DebugEngine', 'scripts.peer_review']:
            logging.getLogger(name).setLevel(logging.ERROR)

        # Step 1: Extract what changed from git
        delta = self.delta_extractor.extract_from_git(staged_only=staged_only)
        
        if not delta.modified_elements:
            return self._create_no_changes_advisory()
        
        # Get old/new code
        old_code, new_code = self._get_code_samples(delta)
        
        # Step 2: Syntax check on the new code
        syntax_errors = self.technical_validator._check_sql_syntax(new_code)
        
        # Step 3: Find which specific tables changed
        # delta.modified_elements already contains table names thanks to block splitting
        changed_tables = list(set(delta.modified_elements))
        
        # Build per-table change descriptions
        table_descriptions = self._describe_table_changes(old_code, new_code)
        
        # Step 4: Trace downstream impact chain
        impact_chains = {}  # {changed_table: [(downstream_table, distance), ...]}
        for table in changed_tables:
            chain = self._trace_full_chain(table)
            if chain:
                impact_chains[table] = chain
        
        # Step 5: Calculate risk level
        risk_level = self._calculate_risk(syntax_errors, changed_tables, impact_chains)
        advisory_msg = self._get_advisory_message(risk_level)
        
        # Build table -> file, per-table "from" summary, and all_blocks (for downstream grain lookup)
        table_to_file: Dict[str, str] = {}
        table_old_summary: Dict[str, str] = {}
        all_blocks: Dict[str, str] = {}
        for filepath, details in delta.details.items():
            if not isinstance(details, dict) or 'new_code' not in details:
                continue
            old_code_f = details.get('old_code', '')
            new_code_f = details.get('new_code', '')
            old_blocks = self._split_sql_into_blocks(old_code_f)
            new_blocks = self._split_sql_into_blocks(new_code_f)
            all_blocks.update(new_blocks)
            for table, block in new_blocks.items():
                table_to_file[table] = filepath
                table_old_summary[table] = self._old_behavior_summary(old_blocks.get(table, ''))
        
        # Business: grain mismatch — e.g. grouping by employee while downstream is company-level (senior DE concern)
        # Only flag when both are entity-level grains (company, employee, department) and they differ
        entity_grains = {"company", "employee", "department"}
        grain_mismatch_notes: List[Tuple[str, str, str, str, str, str]] = []
        for table in changed_tables:
            new_block = all_blocks.get(table)
            if not new_block:
                continue
            new_grain, new_group_by_str = self._infer_grain_and_group_by(new_block, table)
            if not new_grain or new_grain not in entity_grains:
                continue
            for downstream_table, _ in impact_chains.get(table, []):
                down_block = all_blocks.get(downstream_table)
                if not down_block:
                    continue
                down_grain, down_group_by_str = self._infer_grain_and_group_by(down_block, downstream_table)
                if not down_grain or down_grain == new_grain or down_grain not in entity_grains:
                    continue
                grain_mismatch_notes.append((
                    table, downstream_table, new_grain, down_grain,
                    new_group_by_str or "(inferred)", down_group_by_str or "(inferred)"
                ))
        
        # Load business context if available (built by peer-review setup)
        business_context = self._load_peer_review_context()
        
        # Step 6: Format clear report (from→to, impact, incorrect, business, Senior DE thoughts)
        formatted = self._format_clear_report(
            syntax_errors=syntax_errors,
            changed_tables=changed_tables,
            table_descriptions=table_descriptions,
            table_old_summary=table_old_summary,
            table_to_file=table_to_file,
            impact_chains=impact_chains,
            grain_mismatch_notes=grain_mismatch_notes,
            business_context=business_context,
        )
        
        return CommitAdvisory(
            risk_level=risk_level,
            advisory=advisory_msg,
            semantic_delta=delta.to_dict(),
            blast_radius={'impact_chains': {k: v for k, v in impact_chains.items()}},
            technical_report={'syntax_errors': syntax_errors},
            business_report={},
            files_changed=len(delta.details),
            technical_blockers=len(syntax_errors),
            business_impact_summary='',
            formatted_output=formatted
        )

    # ─── Helper methods ─────────────────────────────────────────────

    def _load_peer_review_context(self) -> Optional[Dict[str, Any]]:
        """Load business context from config/peer_review_context.json if present."""
        try:
            base = Path(self.delta_extractor.repo_path) if self.delta_extractor.repo_path else Path(PROJECT_ROOT)
            ctx_path = base / "config" / "peer_review_context.json"
            if ctx_path.exists():
                import json
                return json.loads(ctx_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        return None

    def _get_code_samples(self, delta) -> Tuple[str, str]:
        """Get combined old and new code from ALL files in the delta."""
        if not delta.details:
            return "", ""
        
        all_old = []
        all_new = []
        
        for filepath, details in delta.details.items():
            # If the delta already stored old_code/new_code (from block splitting)
            if isinstance(details, dict) and 'old_code' in details:
                all_old.append(details.get('old_code', ''))
                all_new.append(details.get('new_code', ''))
            else:
                # Read new code from filesystem
                try:
                    fpath = Path(filepath)
                    if fpath.exists():
                        all_new.append(fpath.read_text(encoding='utf-8'))
                except:
                    pass
                
                # Read old code from git
                try:
                    import subprocess
                    result = subprocess.run(
                        ['git', 'show', f'HEAD:{filepath}'],
                        capture_output=True, text=True, cwd='.'
                    )
                    if result.returncode == 0:
                        all_old.append(result.stdout)
                except:
                    pass
        
        return '\n'.join(all_old), '\n'.join(all_new)

    def _old_behavior_summary(self, old_block: str) -> str:
        """One-line summary of old block for 'from' in report."""
        if not (old_block or old_block.strip()):
            return "(none)"
        # Strip line comments so we don't capture comment text as WHERE clause
        block_no_comments = re.sub(r"--[^\n]*", "", old_block)
        u = block_no_comments.upper()
        parts = []
        if "WHERE" in u:
            m = re.search(r"WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|;|\Z)", block_no_comments, re.IGNORECASE | re.DOTALL)
            raw = (m.group(1).strip() if m else "")[:60]
            if raw:
                parts.append("WHERE " + (raw + "..." if len(raw) >= 60 else raw))
            else:
                parts.append("WHERE (clause present)")
        else:
            parts.append("no WHERE")
        joins = len(re.findall(r"\bJOIN\b", block_no_comments, re.IGNORECASE))
        lefts = len(re.findall(r"\bLEFT\s+JOIN\b", block_no_comments, re.IGNORECASE))
        if lefts:
            parts.append(f"{lefts} LEFT JOIN(s), {joins - lefts} other JOIN(s)")
        elif joins:
            parts.append(f"{joins} JOIN(s)")
        if "GROUP BY" in u:
            parts.append("GROUP BY")
        return "; ".join(parts) if parts else "logic defined"

    def _extract_group_by_columns(self, block: str) -> List[str]:
        """Extract GROUP BY column list (normalized: last token if dotted)."""
        if not block or "GROUP BY" not in block.upper():
            return []
        m = re.search(
            r"\bGROUP\s+BY\s+([a-zA-Z0-9_,.\s]+?)(?=\s*HAVING|\s*ORDER|\s*LIMIT|;|\Z)",
            block, re.IGNORECASE | re.DOTALL
        )
        if not m:
            return []
        raw = m.group(1).strip()
        cols = []
        for part in re.split(r"\s*,\s*", raw):
            part = part.strip()
            if not part:
                continue
            # e.g. e.emp_id or company_name -> take last token
            tokens = re.split(r"\s*\.\s*", part)
            cols.append(tokens[-1].lower() if tokens else part.lower())
        return cols

    def _infer_grain_and_group_by(self, block: str, table_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Infer grain from GROUP BY columns and table name. Senior-data-engineer semantics.
        Returns (grain, group_by_summary) or (None, None) if unclear.
        """
        cols = self._extract_group_by_columns(block)
        col_str = " ".join(cols)
        table_lower = table_name.lower()
        grain = None
        # Column-based inference (GROUP BY drives grain)
        if any(x in cols or x in col_str for x in ("company_id", "company_name")):
            grain = "company"
        elif any(x in cols or x in col_str for x in ("emp_id", "full_name")):
            grain = "employee"
        elif any(x in cols or x in col_str for x in ("dept_id", "dept_name")):
            grain = "department"
        elif "industry" in cols or "industry" in col_str:
            grain = "industry"
        elif "job_id" in cols or "job_id" in col_str:
            grain = "job"
        # Table-name heuristic when no GROUP BY or no match
        if not grain:
            if "company" in table_lower and ("stats" in table_lower or "dim" in table_lower):
                grain = "company"
            elif "employee" in table_lower or "career" in table_lower or "dim_employees" in table_lower:
                grain = "employee"
            elif "department" in table_lower or "dept" in table_lower:
                grain = "department"
            elif "industry" in table_lower:
                grain = "industry"
            elif "fact_jobs" in table_lower or "job" in table_lower:
                grain = "job"
        group_by_str = ", ".join(cols) if cols else None
        return (grain, group_by_str)

    def _describe_table_changes(self, old_code: str, new_code: str) -> Dict[str, str]:
        """Build short descriptions of what changed in each table block."""
        descriptions = {}
        
        old_blocks = self._split_sql_into_blocks(old_code)
        new_blocks = self._split_sql_into_blocks(new_code)
        
        all_tables = set(old_blocks.keys()) | set(new_blocks.keys())
        
        for table in all_tables:
            old_block = old_blocks.get(table, "")
            new_block = new_blocks.get(table, "")
            
            if old_block.strip() == new_block.strip():
                continue
            
            if not old_block:
                descriptions[table] = "NEW TABLE added"
                continue
            if not new_block:
                descriptions[table] = "TABLE REMOVED"
                continue
            
            # Detect specific changes
            changes = []
            
            # Aggregation changes — detect swaps first (AVG→SUM etc)
            agg_funcs = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN']
            old_aggs = set(f for f in agg_funcs if f + '(' in old_block.upper())
            new_aggs = set(f for f in agg_funcs if f + '(' in new_block.upper())
            removed_aggs = old_aggs - new_aggs
            added_aggs = new_aggs - old_aggs
            
            # If one was removed and another added, it's a swap
            if removed_aggs and added_aggs:
                for old_f in sorted(removed_aggs):
                    for new_f in sorted(added_aggs):
                        changes.append(f"{old_f}() changed to {new_f}()")
            elif removed_aggs:
                for f in sorted(removed_aggs):
                    changes.append(f"{f}() removed")
            elif added_aggs:
                for f in sorted(added_aggs):
                    changes.append(f"{f}() added")
            
            # WHERE clause changes
            old_where = 'WHERE' in old_block.upper()
            new_where = 'WHERE' in new_block.upper()
            if not old_where and new_where:
                # Extract the WHERE condition
                where_match = re.search(r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|;|\Z)', new_block, re.IGNORECASE | re.DOTALL)
                condition = where_match.group(1).strip() if where_match else ""
                changes.append(f"Added WHERE {condition}")
            elif old_where and not new_where:
                changes.append("WHERE clause removed")
            elif old_where and new_where:
                old_where_text = re.search(r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|;|\Z)', old_block, re.IGNORECASE | re.DOTALL)
                new_where_text = re.search(r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|;|\Z)', new_block, re.IGNORECASE | re.DOTALL)
                if old_where_text and new_where_text and old_where_text.group(1).strip() != new_where_text.group(1).strip():
                    changes.append("WHERE clause modified")
            
            # JOIN changes
            old_joins = len(re.findall(r'\bJOIN\b', old_block, re.IGNORECASE))
            new_joins = len(re.findall(r'\bJOIN\b', new_block, re.IGNORECASE))
            if new_joins > old_joins:
                changes.append(f"Added {new_joins - old_joins} JOIN(s)")
            elif new_joins < old_joins:
                changes.append(f"Removed {old_joins - new_joins} JOIN(s)")
            
            # GROUP BY changes (match only column list: word chars, commas, dots; stop at ), ;, ORDER, HAVING)
            group_by_re = re.compile(r'\bGROUP\s+BY\s+([a-zA-Z0-9_,.\s]+?)(?=\s*[;)]|\s+ORDER|\s+HAVING|\Z)', re.IGNORECASE)
            old_gb = group_by_re.search(old_block)
            new_gb = group_by_re.search(new_block)
            old_gb_cols = old_gb.group(1).strip() if old_gb else None
            new_gb_cols = new_gb.group(1).strip() if new_gb else None
            if old_gb_cols != new_gb_cols:
                if not old_gb_cols:
                    changes.append(f"Added GROUP BY {new_gb_cols}")
                elif not new_gb_cols:
                    changes.append("GROUP BY removed")
                else:
                    changes.append(f"GROUP BY changed from {old_gb_cols} to {new_gb_cols}")
            
            # Column changes
            old_cols = set(re.findall(r'\bAS\s+([a-zA-Z_]\w*)', old_block, re.IGNORECASE))
            new_cols = set(re.findall(r'\bAS\s+([a-zA-Z_]\w*)', new_block, re.IGNORECASE))
            added = new_cols - old_cols
            removed = old_cols - new_cols
            if added:
                changes.append(f"Added column(s): {', '.join(added)}")
            if removed:
                changes.append(f"Removed column(s): {', '.join(removed)}")
            
            if changes:
                descriptions[table] = "; ".join(changes)
            else:
                descriptions[table] = "Logic modified"
        
        return descriptions

    @staticmethod
    def _split_sql_into_blocks(sql: str) -> Dict[str, str]:
        """Split a SQL file into per-table blocks keyed by table name."""
        blocks = {}
        if not sql:
            return blocks
        parts = re.split(r'(?=CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+)', sql, flags=re.IGNORECASE)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            table_match = SQLPatternMatcher.CREATE_TABLE.search(part)
            if table_match:
                blocks[table_match.group(1)] = part
        return blocks

    def _trace_full_chain(self, table: str) -> List[Tuple[str, int]]:
        """Trace all downstream tables recursively with hop distance."""
        if not self.debug_engine:
            return []
        
        chain = []
        visited = set()
        queue = [(table, 0)]
        
        while queue:
            current, distance = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            if distance > 0:  # Don't include the source table itself
                chain.append((current, distance))
            
            try:
                downstream = self.debug_engine.get_downstream_tables(current)
                for child in downstream:
                    if child not in visited:
                        queue.append((child, distance + 1))
            except Exception:
                pass
        
        return chain

    def _calculate_risk(
        self,
        syntax_errors: List[str],
        changed_tables: List[str],
        impact_chains: Dict[str, List]
    ) -> str:
        """Calculate risk level based on findings."""
        # RED: syntax errors
        if syntax_errors:
            return 'RED'
        
        # Count total downstream impact
        total_downstream = sum(len(chain) for chain in impact_chains.values())
        
        # RED: 5+ downstream tables affected
        if total_downstream >= 5:
            return 'RED'
        
        # YELLOW: any downstream impact or 2+ tables changed
        if total_downstream > 0 or len(changed_tables) >= 2:
            return 'YELLOW'
        
        # GREEN: only 1 table changed, no downstream
        return 'GREEN'

    def _get_advisory_message(self, risk_level: str) -> str:
        """Get advisory message based on risk level."""
        return {
            'GREEN': 'Safe to commit',
            'YELLOW': 'Proceed with caution',
            'RED': 'Manual review required'
        }.get(risk_level, 'Unknown')

    def _create_no_changes_advisory(self) -> CommitAdvisory:
        """Create advisory when no changes detected."""
        output = "PEER REVIEW REPORT\n\nNo SQL changes detected.\n---"
        return CommitAdvisory(
            risk_level='GREEN',
            advisory='Safe to commit',
            semantic_delta={},
            blast_radius={},
            technical_report={},
            business_report={},
            files_changed=0,
            technical_blockers=0,
            business_impact_summary='No changes detected',
            formatted_output=output
        )

    def _format_clear_report(
        self,
        syntax_errors: List[str],
        changed_tables: List[str],
        table_descriptions: Dict[str, str],
        table_old_summary: Dict[str, str],
        table_to_file: Dict[str, str],
        impact_chains: Dict[str, List],
        grain_mismatch_notes: List[Tuple[str, str, str, str, str, str]],
        business_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Format a clear report: from→to, impact, incorrect (typos/schema), business, Senior DE thoughts."""
        lines = []
        lines.append("PEER REVIEW REPORT")
        lines.append("")

        # 0. Senior DE thoughts (when business context exists — like a human who knows the project)
        if business_context:
            thoughts = self._senior_de_thoughts(
                business_context=business_context,
                changed_tables=changed_tables,
                table_to_file=table_to_file,
                impact_chains=impact_chains,
                syntax_errors=syntax_errors,
                grain_mismatch_notes=grain_mismatch_notes,
            )
            if thoughts:
                lines.append("SENIOR DATA ENGINEER NOTES:")
                for t in thoughts:
                    lines.append(f"  - {t}")
                lines.append("")

        # 1. For each change: "User changed from X to Y. This change impacts: ..."
        for table in sorted(changed_tables):
            filepath = table_to_file.get(table, "(unknown file)")
            from_summary = table_old_summary.get(table, "(previous logic)")
            to_summary = table_descriptions.get(table, "logic modified")
            impacts = impact_chains.get(table, [])
            downstream = [t for t, _ in impacts]
            impact_str = ", ".join(downstream) if downstream else "no downstream tables"
            lines.append(f"** {table} (file: {filepath})")
            lines.append(f"   Changed from: {from_summary}")
            lines.append(f"   To: {to_summary}")
            lines.append(f"   This change impacts: {impact_str}")
            lines.append("")

        # 2. Incorrect (technical): typo in schema name, SQL keyword typo, etc.
        if syntax_errors:
            lines.append("INCORRECT (technical):")
            for err in syntax_errors:
                if "Unknown schema" in err or "schema" in err.lower():
                    lines.append(f"  - Typo or unknown schema name: {err}")
                elif "typo" in err.lower():
                    lines.append(f"  - SQL keyword typo: {err}")
                else:
                    lines.append(f"  - {err}")
            lines.append("")

        # 3. Business: grain mismatch (senior DE wording)
        if grain_mismatch_notes:
            lines.append("BUSINESS (grain / logic mismatch):")
            for changed_table, downstream_table, changed_grain, down_grain, new_gb, down_gb in grain_mismatch_notes:
                lines.append(
                    f"  - Downstream table {downstream_table} is built at {down_grain}-level grain (GROUP BY {down_gb}). "
                    f"Your change to {changed_table} now produces {changed_grain}-level grain (GROUP BY {new_gb}). "
                    f"Feeding {changed_grain}-level output into a {down_grain}-level table will duplicate or corrupt metrics. "
                    f"Align grains or update the downstream logic."
                )
            lines.append("")

        lines.append("---")
        return "\n".join(lines)

    def _senior_de_thoughts(
        self,
        business_context: Dict[str, Any],
        changed_tables: List[str],
        table_to_file: Dict[str, str],
        impact_chains: Dict[str, List],
        syntax_errors: List[str],
        grain_mismatch_notes: List[Tuple[str, str, str, str, str, str]],
    ) -> List[str]:
        """Build human-like notes from a senior DE who knows the business."""
        thoughts: List[str] = []
        etl_paths = {e["path"] for e in business_context.get("etl_files", []) if e.get("type") in ("etl_sql", "sql")}
        tables_info = business_context.get("tables", {})

        # Which changed files are ETL
        changed_files = list(set(table_to_file.get(t, "") for t in changed_tables if table_to_file.get(t)))
        norm_etl = {p.replace("\\", "/") for p in etl_paths}
        etl_changed = [f for f in changed_files if f and (f.replace("\\", "/") in norm_etl)]
        if etl_changed:
            thoughts.append(f"You changed ETL script(s): {', '.join(etl_changed)}. These drive the pipeline; impact below.")

        # High-impact tables (many downstream)
        for table in changed_tables:
            downstream = impact_chains.get(table, [])
            if len(downstream) >= 2:
                info = tables_info.get(table, {})
                grain = info.get("grain") or "unknown"
                desc = info.get("description") or ""
                label = f" ({desc})" if desc else f" — {grain}-level." if grain != "unknown" else ""
                thoughts.append(f"{table}{label} feeds {len(downstream)} downstream table(s). Confirm metrics and grain stay aligned.")

        # Typos
        if syntax_errors:
            thoughts.append("Fix the technical issues above (typos/schema) before relying on lineage; they can break the build.")

        # Grain mismatch already called out in BUSINESS section
        if grain_mismatch_notes:
            thoughts.append("There is a grain mismatch between a changed table and a downstream table; see BUSINESS section.")

        return thoughts[:5]


if __name__ == '__main__':
    orchestrator = PeerReviewOrchestrator()
    advisory = orchestrator.review_changes(staged_only=False)
    print(advisory.formatted_output)
