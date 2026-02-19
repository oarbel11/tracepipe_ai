"""
╔══════════════════════════════════════════════════════════════════╗
║                  SEMANTIC DELTA EXTRACTOR                        ║
╚══════════════════════════════════════════════════════════════════╝

Extracts meaningful logic changes from code diffs, filtering out noise.

USAGE:
    from peer_review.semantic_delta import SemanticDeltaExtractor
    
    extractor = SemanticDeltaExtractor()
    delta = extractor.extract_from_git()  # Analyze staged changes
    
    print(delta.modified_elements)
    print(delta.logic_delta)
    print(delta.integrity_flag)
"""

import re
import sys
from pathlib import Path
from typing import List, Dict, Set, Optional, Any
from dataclasses import dataclass, asdict
import logging

try:
    import git
except ImportError:
    git = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config.db_config import get_all_etl_dirs
except Exception:
    # Config is optional for semantic delta; we'll fall back to repo root if unavailable
    get_all_etl_dirs = None  # type: ignore


@dataclass
class SemanticDelta:
    """Result of semantic delta extraction."""
    modified_elements: List[str]  # Tables, columns, views modified
    logic_delta: str  # Human-readable description of the change
    integrity_flag: bool  # True if change affects PK/FK/Join keys
    details: Dict[str, Any]  # Additional metadata
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SQL PATTERN MATCHERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SQLPatternMatcher:
    """Identifies significant patterns in SQL code."""
    
    # Column definitions in SELECT
    SELECT_COLUMN = re.compile(
        r'SELECT\s+(?:.*?)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        re.IGNORECASE | re.DOTALL
    )
    
    # JOIN conditions
    JOIN_PATTERN = re.compile(
        r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+.*?ON\s+(.*?)(?:WHERE|GROUP|ORDER|LIMIT|;|\Z)',
        re.IGNORECASE | re.DOTALL
    )
    
    # WHERE clauses
    WHERE_PATTERN = re.compile(
        r'WHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|;|\Z)',
        re.IGNORECASE | re.DOTALL
    )
    
    # CREATE TABLE statements
    CREATE_TABLE = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
        re.IGNORECASE
    )
    
    # Column transformations (calculations)
    COLUMN_CALC = re.compile(
        r'([a-zA-Z_][a-zA-Z0-9_]*)\s*[*/+-]\s*([0-9.]+|[a-zA-Z_][a-zA-Z0-9_]*)',
        re.IGNORECASE
    )
    
    # GROUP BY clauses
    GROUP_BY = re.compile(
        r'GROUP\s+BY\s+(.*?)(?:HAVING|ORDER|LIMIT|;|\Z)',
        re.IGNORECASE | re.DOTALL
    )
    
    @staticmethod
    def extract_table_name(sql: str) -> Optional[str]:
        """Extract the target table name from CREATE statement."""
        match = SQLPatternMatcher.CREATE_TABLE.search(sql)
        return match.group(1) if match else None
    
    @staticmethod
    def extract_joins(sql: str) -> List[Dict[str, str]]:
        """Extract JOIN information."""
        joins = []
        for match in SQLPatternMatcher.JOIN_PATTERN.finditer(sql):
            joins.append({
                'table': match.group(1).strip(),
                'condition': match.group(2).strip()
            })
        return joins
    
    @staticmethod
    def extract_where_clause(sql: str) -> Optional[str]:
        """Extract WHERE clause."""
        match = SQLPatternMatcher.WHERE_PATTERN.search(sql)
        return match.group(1).strip() if match else None
    
    @staticmethod
    def extract_columns(sql: str) -> List[str]:
        """Extract column definitions from SELECT."""
        return [m.group(1) for m in SQLPatternMatcher.SELECT_COLUMN.finditer(sql)]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DIFF ANALYZER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DiffAnalyzer:
    """Analyzes git diffs to identify semantic changes."""
    
    def __init__(self):
        self.matcher = SQLPatternMatcher()
    
    def is_noise(self, line: str) -> bool:
        """Check if a line is just noise (whitespace, comments, etc.)."""
        line = line.strip()
        
        # Empty lines
        if not line:
            return True
        
        # Comments (SQL)
        if line.startswith('--') or line.startswith('/*') or line.startswith('*'):
            return True
        
        # Comments (Python)
        if line.startswith('#'):
            return True
        
        # Pure whitespace changes
        if not line.replace(' ', '').replace('\t', ''):
            return True
        
        return False
    
    def analyze_sql_diff(self, old_code: str, new_code: str, filename: str) -> Dict[str, Any]:
        """Analyze differences between old and new SQL code."""
        changes = {
            'table': None,
            'columns_modified': [],
            'joins_modified': [],
            'where_modified': False,
            'where_old': None,
            'where_new': None,
            'has_integrity_impact': False,
            'description': []
        }
        
        # Extract table name
        changes['table'] = self.matcher.extract_table_name(new_code) or \
                          self.matcher.extract_table_name(old_code)
        
        # Compare JOINs
        old_joins = self.matcher.extract_joins(old_code)
        new_joins = self.matcher.extract_joins(new_code)
        
        if old_joins != new_joins:
            changes['joins_modified'] = new_joins
            changes['has_integrity_impact'] = True
            
            # Describe the change
            if len(new_joins) > len(old_joins):
                changes['description'].append(f"Added {len(new_joins) - len(old_joins)} JOIN(s)")
            elif len(new_joins) < len(old_joins):
                changes['description'].append(f"Removed {len(old_joins) - len(new_joins)} JOIN(s)")
            else:
                changes['description'].append("Modified JOIN condition(s)")
        
        # Compare WHERE clauses
        old_where = self.matcher.extract_where_clause(old_code)
        new_where = self.matcher.extract_where_clause(new_code)
        
        if old_where != new_where:
            changes['where_modified'] = True
            changes['where_old'] = old_where
            changes['where_new'] = new_where
            changes['description'].append("Modified filter conditions (WHERE clause)")
        
        # Compare columns
        old_cols = set(self.matcher.extract_columns(old_code))
        new_cols = set(self.matcher.extract_columns(new_code))
        
        added_cols = new_cols - old_cols
        removed_cols = old_cols - new_cols
        
        if added_cols:
            changes['columns_modified'].extend(list(added_cols))
            changes['description'].append(f"Added column(s): {', '.join(added_cols)}")
        
        if removed_cols:
            changes['columns_modified'].extend(list(removed_cols))
            changes['description'].append(f"Removed column(s): {', '.join(removed_cols)}")
        
        return changes


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SEMANTIC DELTA EXTRACTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SemanticDeltaExtractor:
    """
    Extracts semantic changes from code diffs.
    
    Focuses on:
    - Column logic changes
    - Filter conditions (WHERE)
    - Join keys
    
    Ignores:
    - Whitespace
    - Comments
    - Formatting
    """
    
    def __init__(self, repo_path: Optional[str] = None):
        """
        Initialize extractor.
        
        Args:
            repo_path: Path to git repository (defaults to current directory)
        """
        self.repo_path = Path(repo_path) if repo_path else Path.cwd()
        self.analyzer = DiffAnalyzer()
        self.repo = None
        
        if git:
            try:
                self.repo = git.Repo(self.repo_path, search_parent_directories=True)
            except Exception as e:
                logger.warning(f"Could not initialize git repo: {e}")
    
    def extract_from_git(self, staged_only: bool = True) -> SemanticDelta:
        """
        Extract semantic delta by comparing current files on disk to HEAD.
        
        Simple approach: Always compare current file state vs HEAD, regardless of git staging.
        This ensures we detect changes whenever a user saves a file.
        
        Args:
            staged_only: Ignored - we always compare current state vs HEAD
        
        Returns:
            SemanticDelta object with analysis results
        """
        # Discover all SQL files in ETL directories
        sql_files = self._discover_local_sql_files()
        
        # Filter for SQL files only
        relevant_files = [
            f for f in sql_files
            if f.endswith('.sql') and not f.endswith('__pycache__')
        ]
        
        # If discovery found nothing, try direct scan as fallback
        if not relevant_files:
            logger.debug("No files found via discovery, trying direct scan")
            try:
                for sql_file in Path(self.repo_path).rglob("*.sql"):
                    if sql_file.is_file():
                        try:
                            rel_path = sql_file.relative_to(self.repo_path)
                            relevant_files.append(str(rel_path))
                        except ValueError:
                            relevant_files.append(str(sql_file))
            except Exception as e:
                logger.debug(f"Fallback scan failed: {e}")
        
        logger.info(f"Found {len(relevant_files)} SQL file(s) to check")
        if len(relevant_files) > 0:
            logger.debug(f"Files to check: {relevant_files[:3]}")  # Show first 3
        else:
            logger.warning("No SQL files found - discovery may have failed")
        
        if not relevant_files:
            return SemanticDelta(
                modified_elements=[],
                logic_delta="No SQL files found for analysis",
                integrity_flag=False,
                details={}
            )
        
        # Compare each file's current state vs HEAD
        changed_files = []
        for filepath in relevant_files:
            try:
                # Handle both relative and absolute paths
                if Path(filepath).is_absolute():
                    file_path = Path(filepath)
                else:
                    file_path = Path(self.repo_path / filepath)
                
                if not file_path.exists():
                    logger.debug(f"File does not exist: {file_path} (tried from {filepath})")
                    continue
                
                current_content = file_path.read_text(encoding='utf-8')
                
                # Get HEAD version (if git exists and file is tracked)
                head_content = ""
                file_in_head = False
                if self.repo:
                    try:
                        # Try with the filepath as-is (relative to repo root)
                        head_content = self.repo.git.show(f'HEAD:{filepath}')
                        file_in_head = True
                    except Exception as e:
                        # File doesn't exist in HEAD (new file) - this is a change
                        head_content = ""
                        file_in_head = False
                        logger.debug(f"File {filepath} not in HEAD: {e}")
                
                # Compare: if different, it's a change
                # Direct comparison - any difference means change
                if current_content != head_content:
                    changed_files.append(filepath)
                    logger.info(f"✓ Detected changes in {filepath} (in HEAD: {file_in_head}, sizes: {len(current_content)} vs {len(head_content)})")
                elif not file_in_head:
                    # New file (not in HEAD) - always a change
                    changed_files.append(filepath)
                    logger.info(f"✓ Detected new file: {filepath}")
                else:
                    logger.debug(f"No changes detected in {filepath} (both {len(current_content)} chars)")
            except Exception as e:
                logger.warning(f"Error checking {filepath}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                # If we can't check, assume it might have changes to be safe
                changed_files.append(filepath)
        
        # SIMPLE APPROACH: Always scan for SQL files when peer review is requested
        # This ensures we detect changes regardless of git state
        # The user wants peer review to work whenever they save changes
        files_to_analyze = []
        
        # Always do a direct scan first - most reliable
        try:
            for sql_file in Path(self.repo_path).rglob("*.sql"):
                if sql_file.is_file():
                    try:
                        rel_path = sql_file.relative_to(self.repo_path)
                        files_to_analyze.append(str(rel_path).replace('\\', '/'))  # Normalize path separators
                    except ValueError:
                        # File is outside repo_path, use absolute path
                        files_to_analyze.append(str(sql_file))
            if files_to_analyze:
                logger.info(f"Found {len(files_to_analyze)} SQL files via direct scan: {files_to_analyze[:3]}")
        except Exception as e:
            logger.debug(f"Direct scan failed: {e}")
        
        # Fallback to discovery results if direct scan found nothing
        if not files_to_analyze:
            files_to_analyze = changed_files if changed_files else relevant_files
            if files_to_analyze:
                logger.info(f"Using {len(files_to_analyze)} files from discovery: {files_to_analyze[:3]}")
        
        # Final fallback: check config-based ETL directory
        if not files_to_analyze:
            logger.debug("Trying config-based ETL directory scan")
            try:
                if get_all_etl_dirs:
                    etl_dirs = get_all_etl_dirs()
                    for dir_name, dir_path in etl_dirs.items():
                        if dir_path and Path(dir_path).exists():
                            for sql_file in Path(dir_path).rglob("*.sql"):
                                if sql_file.is_file():
                                    try:
                                        rel_path = sql_file.relative_to(self.repo_path)
                                        files_to_analyze.append(str(rel_path).replace('\\', '/'))
                                    except ValueError:
                                        files_to_analyze.append(str(sql_file))
                            if files_to_analyze:
                                logger.info(f"Found {len(files_to_analyze)} SQL files in {dir_name}")
                                break
            except Exception as e:
                logger.debug(f"Config-based scan failed: {e}")
        
        if not files_to_analyze:
            return SemanticDelta(
                modified_elements=[],
                logic_delta="No SQL files found for analysis",
                integrity_flag=False,
                details={}
            )
        
        logger.info(f"Analyzing {len(files_to_analyze)} SQL file(s) for peer review: {files_to_analyze[:3]}")
        
        # FIRST: Extract tables directly from files (bypass git comparison)
        # This ensures peer review always works when files exist
        all_tables_found = []
        file_details = {}
        
        for filepath in files_to_analyze:
            try:
                # Handle both relative and absolute paths, normalize separators
                filepath_normalized = filepath.replace('\\', '/')
                if Path(filepath).is_absolute():
                    file_path = Path(filepath)
                else:
                    file_path = Path(self.repo_path) / filepath_normalized
                
                if not file_path.exists():
                    logger.warning(f"File does not exist: {file_path} (from {filepath})")
                    continue
                
                content = file_path.read_text(encoding='utf-8')
                if not content.strip():
                    logger.debug(f"File is empty: {filepath}")
                    continue
                
                # Extract all table names from CREATE TABLE statements
                table_matches = SQLPatternMatcher.CREATE_TABLE.findall(content)
                
                if not table_matches:
                    logger.debug(f"No CREATE TABLE statements found in {filepath}")
                else:
                    for table_match in table_matches:
                        if table_match not in all_tables_found:
                            all_tables_found.append(table_match)
                    
                    # Get HEAD version for comparison
                    head_content = ""
                    if self.repo:
                        try:
                            head_content = self.repo.git.show(f'HEAD:{filepath_normalized}')
                        except:
                            head_content = ""
                    
                    # Store file details
                    file_details[filepath_normalized] = {
                        'old_code': head_content,
                        'new_code': content
                    }
                    
                    logger.info(f"✓ Found {len(table_matches)} table(s) in {filepath}: {table_matches[:3]}")
            except Exception as e:
                logger.error(f"Error processing {filepath}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        # Analyze the files (compare current vs HEAD, or treat as new if not in HEAD)
        result = self._analyze_files(files_to_analyze, staged=False)
        
        # ALWAYS override with direct table extraction to ensure peer review works
        # This is the key fix: bypass git comparison issues by always extracting tables
        if all_tables_found:
            result.modified_elements = all_tables_found
            result.details.update(file_details)
            result.logic_delta = f"Analyzed {len(all_tables_found)} table(s) across {len(files_to_analyze)} file(s)"
            logger.info(f"✓ Peer review ready: {len(all_tables_found)} table(s): {all_tables_found[:5]}")
        elif files_to_analyze:
            # Fallback: if extraction failed but files exist, something went wrong
            logger.error(f"CRITICAL: Table extraction returned empty but {len(files_to_analyze)} file(s) exist!")
            logger.error(f"Files: {files_to_analyze[:3]}")
            # Still try to return something so peer review doesn't fail completely
            result.modified_elements = ["unknown"]  # Placeholder to trigger review
            result.logic_delta = f"Files found but table extraction failed - {len(files_to_analyze)} file(s)"
        
        # Final check: ensure we have results
        if not result.modified_elements:
            logger.error("FATAL: No modified elements after all extraction attempts!")
            logger.error(f"Files to analyze: {files_to_analyze}")
            logger.error(f"Tables found: {all_tables_found}")
        
        return result

    def extract_from_files(self, old_file: str, new_file: str) -> SemanticDelta:
        """
        Extract semantic delta from two file versions.
        
        Args:
            old_file: Path to old version
            new_file: Path to new version
        
        Returns:
            SemanticDelta object
        """
        with open(old_file, 'r', encoding='utf-8') as f:
            old_code = f.read()
        
        with open(new_file, 'r', encoding='utf-8') as f:
            new_code = f.read()
        
        filename = Path(new_file).name
        
        if filename.endswith('.sql'):
            changes = self.analyzer.analyze_sql_diff(old_code, new_code, filename)
            return self._build_delta(filename, changes)
        
        return SemanticDelta(
            modified_elements=[],
            logic_delta="No analysis available for this file type",
            integrity_flag=False,
            details={}
        )
    
    @staticmethod
    def _split_sql_into_blocks(sql: str) -> Dict[str, str]:
        """Split a SQL file into per-table blocks keyed by table name."""
        blocks = {}
        if not sql:
            return blocks
        # Split on CREATE statements, keeping the delimiter
        parts = re.split(r'(?=CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+)', sql, flags=re.IGNORECASE)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            table_match = SQLPatternMatcher.CREATE_TABLE.search(part)
            if table_match:
                table_name = table_match.group(1)
                blocks[table_name] = part
        return blocks

    def _analyze_files(self, files: List[str], staged: bool) -> SemanticDelta:
        """Analyze multiple changed files."""
        all_modified_elements = []
        all_descriptions = []
        has_integrity_impact = False
        all_details = {}
        
        for filepath in files:
            try:
                # Get old and new content
                if staged:
                    # Compare staged with HEAD
                    try:
                        old_content = self.repo.git.show(f'HEAD:{filepath}')
                    except:
                        old_content = ""  # New file
                    
                    new_content = Path(self.repo_path / filepath).read_text(encoding='utf-8')
                else:
                    # Compare working directory with HEAD
                    try:
                        old_content = self.repo.git.show(f'HEAD:{filepath}')
                    except Exception as e:
                        # File doesn't exist in HEAD (new file) or other error
                        logger.debug(f"Could not get HEAD version of {filepath}: {e}")
                        old_content = ""  # New file
                    
                    try:
                        new_content = Path(self.repo_path / filepath).read_text(encoding='utf-8')
                    except Exception as e:
                        logger.error(f"Could not read file {filepath}: {e}")
                        continue
                
                # Analyze based on file type
                if filepath.endswith('.sql'):
                    # Split into per-table blocks for accurate analysis
                    old_blocks = self._split_sql_into_blocks(old_content)
                    new_blocks = self._split_sql_into_blocks(new_content)
                    
                    # Find all tables that exist in either version
                    all_tables = set(old_blocks.keys()) | set(new_blocks.keys())
                    
                    # If no tables found, treat entire file as one change
                    if not all_tables and (old_content or new_content):
                        logger.debug(f"No table blocks found in {filepath}, analyzing entire file")
                        changes = self.analyzer.analyze_sql_diff(old_content, new_content, filepath)
                        if changes.get('description'):
                            all_descriptions.extend(changes['description'])
                        has_integrity_impact = has_integrity_impact or changes.get('has_integrity_impact', False)
                        # Extract table names from CREATE statements
                        table_matches = SQLPatternMatcher.CREATE_TABLE.findall(new_content)
                        for table_match in table_matches:
                            all_modified_elements.append(table_match)
                    
                    for table_name in all_tables:
                        old_block = old_blocks.get(table_name, "")
                        new_block = new_blocks.get(table_name, "")
                        
                        # Always analyze blocks - even if they match HEAD, peer review should still run
                        # This ensures peer review works whenever the user requests it
                        if old_block == new_block:
                            # Blocks match, but still include the table for review
                            logger.debug(f"Block {table_name} matches HEAD, but including for review")
                            all_modified_elements.append(table_name)
                            # Still analyze to get table structure info
                            changes = self.analyzer.analyze_sql_diff(new_block, new_block, filepath)
                            changes['table'] = table_name
                            if changes.get('description'):
                                prefixed = [f"[{table_name}] Current state" for _ in changes['description']]
                                all_descriptions.extend(prefixed)
                            continue
                        
                        # Log what we're comparing
                        logger.debug(f"Comparing block for {table_name}: old={len(old_block)} chars, new={len(new_block)} chars")
                        
                        # Analyze this specific block
                        changes = self.analyzer.analyze_sql_diff(old_block, new_block, filepath)
                        # Override table name with the one we know
                        changes['table'] = table_name
                        
                        # Accumulate results
                        all_modified_elements.append(table_name)
                        for col in changes['columns_modified']:
                            all_modified_elements.append(f"{table_name}.{col}")
                        
                        if changes['description']:
                            prefixed = [f"[{table_name}] {d}" for d in changes['description']]
                            all_descriptions.extend(prefixed)
                        
                        has_integrity_impact = has_integrity_impact or changes['has_integrity_impact']
                    
                    # Store file-level details with the new code for business/tech validation
                    all_details[filepath] = {
                        'old_code': old_content,
                        'new_code': new_content,
                    }
            
            except Exception as e:
                logger.error(f"Error analyzing {filepath}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        # Build final delta
        logic_delta = "; ".join(all_descriptions) if all_descriptions else "No significant logic changes detected"
        
        return SemanticDelta(
            modified_elements=all_modified_elements,
            logic_delta=logic_delta,
            integrity_flag=has_integrity_impact,
            details=all_details
        )
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FALLBACK: FILESYSTEM-ONLY ANALYSIS (NO GIT)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def _discover_local_sql_files(self) -> List[str]:
        """
        Discover SQL files to analyze when git is not available.
        
        Strategy:
        - Prefer ETL directories from config (sql_dir / jobs_dir / notebooks_dir)
        - Fall back to scanning the current repo_path for *.sql
        """
        roots: List[Path] = []
        
        # Try to use configured ETL directories first
        if get_all_etl_dirs is not None:
            try:
                dirs = get_all_etl_dirs()  # type: ignore[operator]
                for p in dirs.values():
                    if p is not None:
                        path_obj = Path(p)
                        if path_obj.exists():
                            roots.append(path_obj)
            except Exception as e:
                logger.warning(f"Could not read ETL dirs from config: {e}")
        
        # Fallback: use repo_path itself
        if not roots:
            roots.append(self.repo_path)
        
        files: Set[str] = set()
        for root in roots:
            try:
                for p in root.rglob("*.sql"):
                    # Prefer paths relative to repo root when possible
                    try:
                        rel = p.relative_to(self.repo_path)
                        files.add(str(rel))
                    except ValueError:
                        files.add(str(p))
            except Exception as e:
                logger.warning(f"Error scanning {root} for SQL files: {e}")
        
        return sorted(files)
    
    def _extract_from_filesystem(self) -> SemanticDelta:
        """
        Fallback path when git is not available.
        
        We can't see *what* changed without git history, so we:
        - Treat all discovered SQL files as "current truth"
        - Mark each table found as a modified element
        - Provide basic descriptions so downstream tools still work
        """
        files = self._discover_local_sql_files()
        
        if not files:
            return SemanticDelta(
                modified_elements=[],
                logic_delta="No SQL files found for analysis",
                integrity_flag=False,
                details={}
            )
        
        all_modified_elements: List[str] = []
        all_descriptions: List[str] = []
        all_details: Dict[str, Any] = {}
        
        for filepath in files:
            try:
                full_path = Path(filepath)
                if not full_path.is_absolute():
                    full_path = self.repo_path / filepath
                
                if not full_path.exists():
                    continue
                
                new_content = full_path.read_text(encoding='utf-8')
                
                # Split into per-table blocks
                blocks = self._split_sql_into_blocks(new_content)
                for table_name, block in blocks.items():
                    all_modified_elements.append(table_name)
                    all_descriptions.append(f"[{table_name}] Existing table definition scanned from filesystem")
                
                all_details[str(filepath)] = {
                    'old_code': "",
                    'new_code': new_content,
                }
            except Exception as e:
                logger.error(f"Error analyzing {filepath} in filesystem mode: {e}")
        
        # Deduplicate elements while preserving order
        seen: Set[str] = set()
        unique_elements: List[str] = []
        for el in all_modified_elements:
            if el not in seen:
                seen.add(el)
                unique_elements.append(el)
        
        logic_delta = "; ".join(all_descriptions) if all_descriptions else "No significant logic changes detected"
        
        return SemanticDelta(
            modified_elements=unique_elements,
            logic_delta=logic_delta,
            integrity_flag=False,
            details=all_details
        )
    
    def _build_delta(self, filename: str, changes: Dict[str, Any]) -> SemanticDelta:
        """Build SemanticDelta from analysis results."""
        modified_elements = []
        
        if changes['table']:
            for col in changes['columns_modified']:
                modified_elements.append(f"{changes['table']}.{col}")
            if not changes['columns_modified']:
                modified_elements.append(changes['table'])
        
        logic_delta = "; ".join(changes['description']) if changes['description'] else \
                     "No significant logic changes detected"
        
        return SemanticDelta(
            modified_elements=modified_elements,
            logic_delta=logic_delta,
            integrity_flag=changes['has_integrity_impact'],
            details={filename: changes}
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI / Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    
    # Test with current repo
    extractor = SemanticDeltaExtractor()
    delta = extractor.extract_from_git(staged_only=True)
    
    print("\n" + "="*70)
    print("SEMANTIC DELTA EXTRACTION RESULTS")
    print("="*70)
    print(json.dumps(delta.to_dict(), indent=2))
