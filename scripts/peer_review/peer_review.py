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
from typing import Optional, Dict, Any, List, Set, Tuple
from dataclasses import dataclass, asdict
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.peer_review.semantic_delta import SemanticDeltaExtractor, SQLPatternMatcher
from scripts.peer_review.blast_radius import BlastRadiusMapper
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
        
        # Initialize components
        self.delta_extractor = SemanticDeltaExtractor(repo_path)
        self.blast_mapper = BlastRadiusMapper(self.debug_engine)
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
        
        # Step 6: Format output
        formatted = self._format_output(
            risk_level=risk_level,
            advisory=advisory_msg,
            files=list(delta.details.keys()),
            syntax_errors=syntax_errors,
            changed_tables=changed_tables,
            table_descriptions=table_descriptions,
            impact_chains=impact_chains
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
        output = []
        output.append("")
        output.append("╔" + "="*68 + "╗")
        output.append("║" + " "*20 + "SENIOR PEER REVIEW" + " "*30 + "║")
        output.append("╚" + "="*68 + "╝")
        output.append("")
        output.append("✅ No SQL changes detected. You're good!")
        output.append("")
        output.append("─" * 70)
        
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
            formatted_output="\n".join(output)
        )

    def _format_output(
        self,
        risk_level: str,
        advisory: str,
        files: List[str],
        syntax_errors: List[str],
        changed_tables: List[str],
        table_descriptions: Dict[str, str],
        impact_chains: Dict[str, List]
    ) -> str:
        """Format the clean peer review output."""
        
        risk_emoji = {'GREEN': '🟢', 'YELLOW': '🟡', 'RED': '🔴'}
        
        output = []
        output.append("")
        output.append("╔" + "="*68 + "╗")
        output.append("║" + " "*20 + "SENIOR PEER REVIEW" + " "*30 + "║")
        output.append("╚" + "="*68 + "╝")
        output.append("")
        
        # Risk level
        output.append(f"Risk Level: {risk_emoji.get(risk_level, '⚪')} {risk_level} - {advisory}")
        output.append("")
        
        # ── Section 1: Syntax Errors (only if found) ──
        if syntax_errors:
            output.append("⚠️  Syntax Errors:")
            output.append("   These are bugs that will BREAK your code if deployed.")
            for error in syntax_errors:
                output.append(f"   🔴 {error}")
            output.append("")
        
        # ── Section 2: Directly Changed Tables ──
        output.append("📋 Directly Changed Tables:")
        output.append("   These are the tables whose logic you modified.")
        if changed_tables:
            for table in sorted(changed_tables):
                desc = table_descriptions.get(table, "")
                if desc:
                    output.append(f"   • {table}  — {desc}")
                else:
                    output.append(f"   • {table}")
        else:
            output.append("   (none)")
        output.append("")
        
        # ── Section 3: Impact Chain (Downstream Dominos) ──
        output.append("🔗 Impact Chain (Downstream Dominos):")
        output.append("   These tables DEPEND on the ones you changed — they'll be affected too.")
        
        has_any_chain = any(len(chain) > 0 for chain in impact_chains.values())
        
        if has_any_chain:
            output.append("")
            for table in sorted(impact_chains.keys()):
                chain = impact_chains[table]
                if not chain:
                    continue
                output.append(f"   {table}")
                for i, (downstream, distance) in enumerate(chain):
                    is_last = (i == len(chain) - 1)
                    connector = "└─→" if is_last else "├─→"
                    hop_label = "1 hop - direct dependency" if distance == 1 else f"{distance} hops away"
                    output.append(f"     {connector} {downstream} ({hop_label})")
                output.append("")
        else:
            output.append("   ✅ No downstream dependencies found.")
            # Check if lineage metadata exists
            has_lineage = False
            if self.debug_engine:
                try:
                    meta = self.debug_engine._check_metadata_exists()
                    has_lineage = meta.get('table_lineage', False)
                except:
                    pass
            if not has_lineage:
                output.append("   💡 Run 'python scripts/cli.py build' to build lineage metadata.")
            output.append("")
        
        # ── Section 4: Advisory ──
        output.append("💡 Advisory:")
        if risk_level == 'RED':
            output.append(f"   {advisory.upper()}")
            output.append("")
            output.append("   Recommended actions:")
            if syntax_errors:
                output.append("   1. Fix syntax errors before committing")
            else:
                output.append("   1. Review all impacted downstream tables")
                output.append("   2. Test changes in staging environment")
                output.append("   3. Notify stakeholders of metric changes")
        elif risk_level == 'YELLOW':
            output.append(f"   {advisory}")
            output.append("")
            output.append("   Recommended actions:")
            output.append("   1. Review downstream impact chain above")
            output.append("   2. Verify downstream tables still produce correct results")
            output.append("   3. Monitor metrics after deployment")
        else:
            output.append(f"   ✅ {advisory}")
            output.append("   No significant risks detected.")
        
        output.append("")
        output.append("─" * 70)
        
        return "\n".join(output)

    def _format_no_changes(self) -> str:
        """Format output when no changes detected."""
        output = []
        output.append("")
        output.append("╔" + "="*68 + "╗")
        output.append("║" + " "*20 + "SENIOR PEER REVIEW" + " "*30 + "║")
        output.append("╚" + "="*68 + "╝")
        output.append("")
        output.append("✅ No SQL changes detected. You're good!")
        output.append("")
        output.append("─" * 70)
        return "\n".join(output)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI / Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    
    orchestrator = PeerReviewOrchestrator()
    advisory = orchestrator.review_changes(staged_only=False)
    
    print(advisory.formatted_output)
    
    # Also save JSON for debugging
    with open('peer_review_result.json', 'w') as f:
        json.dump(advisory.to_dict(), f, indent=2)
    
    print("\nDetailed results saved to peer_review_result.json")
