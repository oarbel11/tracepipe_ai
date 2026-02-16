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
import ast
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
        Extract semantic delta from git staged changes.
        
        Args:
            staged_only: Only analyze staged files (True) or all modified files (False)
        
        Returns:
            SemanticDelta object with analysis results
        """
        if not self.repo:
            logger.error("Git repository not available")
            return SemanticDelta(
                modified_elements=[],
                logic_delta="Error: Git not available",
                integrity_flag=False,
                details={}
            )
        
        # Get changed files
        if staged_only:
            # Get staged files
            changed_files = [item.a_path for item in self.repo.index.diff("HEAD")]
        else:
            # Get all modified files (tracked)
            changed_files = [item.a_path for item in self.repo.index.diff(None)]
            # Also include untracked files (new files not yet git-added)
            untracked = [f for f in self.repo.untracked_files 
                         if f.endswith(('.sql', '.py'))]
            changed_files.extend(untracked)
        
        # Filter for SQL/Python files
        relevant_files = [
            f for f in changed_files
            if f.endswith(('.sql', '.py')) and not f.endswith('__pycache__')
        ]
        
        logger.info(f"Analyzing {len(relevant_files)} file(s): {relevant_files}")
        
        return self._analyze_files(relevant_files, staged_only)
    
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
                    # Compare working directory with index
                    try:
                        old_content = self.repo.git.show(f':{filepath}')
                    except:
                        old_content = ""
                    
                    new_content = Path(self.repo_path / filepath).read_text(encoding='utf-8')
                
                # Analyze based on file type
                if filepath.endswith('.sql'):
                    # Split into per-table blocks for accurate analysis
                    old_blocks = self._split_sql_into_blocks(old_content)
                    new_blocks = self._split_sql_into_blocks(new_content)
                    
                    # Find all tables that exist in either version
                    all_tables = set(old_blocks.keys()) | set(new_blocks.keys())
                    
                    for table_name in all_tables:
                        old_block = old_blocks.get(table_name, "")
                        new_block = new_blocks.get(table_name, "")
                        
                        # Skip if no change in this block
                        if old_block.strip() == new_block.strip():
                            continue
                        
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
        
        # Build final delta
        logic_delta = "; ".join(all_descriptions) if all_descriptions else "No significant logic changes detected"
        
        return SemanticDelta(
            modified_elements=all_modified_elements,
            logic_delta=logic_delta,
            integrity_flag=has_integrity_impact,
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
