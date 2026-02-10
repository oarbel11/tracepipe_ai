"""
╔══════════════════════════════════════════════════════════════════╗
║                   TECHNICAL VALIDATOR                            ║
╚══════════════════════════════════════════════════════════════════╝

Predicts technical failures: schema drift, type conflicts, volume anomalies.

USAGE:
    from peer_review.technical_validator import TechnicalValidator
    
    validator = TechnicalValidator()
    report = validator.validate(
        modified_elements=['silver.orders.total_amount'],
        impacted_nodes=['gold.revenue_dashboard'],
        old_code=old_sql,
        new_code=new_sql
    )
"""

import sys
import re
from pathlib import Path
from typing import List, Dict, Set, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.debug_engine import DebugEngine
from config.db_config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class TechnicalBlocker:
    """Represents a potential technical failure."""
    severity: str  # 'HIGH', 'MEDIUM', 'LOW'
    blocker_type: str  # 'type_conflict', 'schema_drift', 'volume_anomaly'
    message: str  # Human-readable description
    details: Dict[str, Any]  # Additional context
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class TechnicalValidationReport:
    """Result of technical validation."""
    technical_blockers: List[Dict[str, Any]]
    has_high_severity: bool
    has_medium_severity: bool
    risk_level: str  # 'HIGH', 'MEDIUM', 'LOW', 'NONE'
    summary: str
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCHEMA ANALYZER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SchemaAnalyzer:
    """Analyzes schema changes for potential conflicts."""
    
    def __init__(self, debug_engine: Optional[DebugEngine] = None):
        """Initialize with DebugEngine."""
        self.engine = debug_engine
    
    def get_table_schema(self, table: str) -> Dict[str, str]:
        """
        Get schema for a table.
        
        Returns:
            Dictionary mapping column_name -> data_type
        """
        if not self.engine:
            return {}
        
        try:
            columns = self.engine.get_table_columns(table)
            schema = {}
            for col in columns:
                if isinstance(col, dict):
                    schema[col.get('column_name', '')] = col.get('data_type', 'UNKNOWN')
                elif hasattr(col, 'column_name'):
                    schema[col.column_name] = getattr(col, 'data_type', 'UNKNOWN')
            return schema
        except Exception as e:
            logger.debug(f"Could not get schema for {table}: {e}")
            return {}
    
    def detect_type_conflicts(
        self,
        modified_table: str,
        modified_columns: List[str],
        downstream_tables: List[str]
    ) -> List[TechnicalBlocker]:
        """
        Detect type conflicts in downstream tables.
        
        Args:
            modified_table: Table being modified
            modified_columns: Columns that changed
            downstream_tables: Tables that depend on modified table
        
        Returns:
            List of type conflict blockers
        """
        blockers = []
        
        # Get schema of modified table
        modified_schema = self.get_table_schema(modified_table)
        
        if not modified_schema:
            return blockers
        
        # Check each downstream table
        for downstream_table in downstream_tables:
            try:
                # Get lineage to see which columns are used
                if self.engine:
                    column_lineage = self.engine.trace_column_lineage(
                        downstream_table,
                        depth=1
                    )
                    
                    # Check for type mismatches
                    for lineage_item in column_lineage:
                        source_table = lineage_item.get('source_table', '')
                        source_column = lineage_item.get('source_column', '')
                        
                        if source_table == modified_table and source_column in modified_columns:
                            # This column is used - check for potential conflicts
                            # (This is simplified - in reality would compare old vs new type)
                            blocker = self._check_column_usage_conflict(
                                modified_table,
                                source_column,
                                downstream_table,
                                lineage_item
                            )
                            if blocker:
                                blockers.append(blocker)
            
            except Exception as e:
                logger.debug(f"Error checking {downstream_table}: {e}")
        
        return blockers
    
    def _check_column_usage_conflict(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        lineage_item: Dict
    ) -> Optional[TechnicalBlocker]:
        """Check if column usage could cause a conflict."""
        # Get transformation logic
        transformation = lineage_item.get('transformation_logic', '')
        
        # Check for operations that require specific types
        type_sensitive_ops = [
            (r'\bSUM\s*\(', 'numeric type for SUM operation'),
            (r'\bAVG\s*\(', 'numeric type for AVG operation'),
            (r'\bCOUNT\s*\(', 'any type for COUNT operation'),
            (r'\bMAX\s*\(', 'comparable type for MAX operation'),
            (r'\bMIN\s*\(', 'comparable type for MIN operation'),
            (r'JOIN.*ON.*=', 'compatible types for JOIN condition'),
        ]
        
        for pattern, operation_desc in type_sensitive_ops:
            if re.search(pattern, transformation, re.IGNORECASE):
                # Found a potentially problematic operation
                # In a real implementation, would compare actual type changes
                # For now, flag it as potential risk
                return TechnicalBlocker(
                    severity='MEDIUM',
                    blocker_type='type_conflict',
                    message=f"Column {source_table}.{source_column} type change may break {operation_desc} in {target_table}",
                    details={
                        'source_table': source_table,
                        'source_column': source_column,
                        'target_table': target_table,
                        'operation': operation_desc,
                        'transformation': transformation
                    }
                )
        
        return None
    
    def detect_schema_drift(
        self,
        old_code: str,
        new_code: str,
        downstream_tables: List[str]
    ) -> List[TechnicalBlocker]:
        """
        Detect schema drift (removed columns that downstream needs).
        
        Args:
            old_code: Previous SQL code
            new_code: New SQL code
            downstream_tables: Tables that depend on this
        
        Returns:
            List of schema drift blockers
        """
        blockers = []
        
        # Extract columns from SELECT statements
        old_columns = self._extract_select_columns(old_code)
        new_columns = self._extract_select_columns(new_code)
        
        # Find removed columns
        removed_columns = old_columns - new_columns
        
        if not removed_columns:
            return blockers
        
        # Check if any downstream table uses these columns
        for downstream_table in downstream_tables:
            try:
                if self.engine:
                    column_lineage = self.engine.trace_column_lineage(downstream_table, depth=2)
                    
                    for lineage_item in column_lineage:
                        source_column = lineage_item.get('source_column', '')
                        
                        if source_column in removed_columns:
                            blockers.append(TechnicalBlocker(
                                severity='HIGH',
                                blocker_type='schema_drift',
                                message=f"Column '{source_column}' removed but required by {downstream_table}",
                                details={
                                    'removed_column': source_column,
                                    'downstream_table': downstream_table,
                                    'usage': lineage_item.get('transformation_logic', 'Unknown')
                                }
                            ))
            
            except Exception as e:
                logger.debug(f"Error checking schema drift for {downstream_table}: {e}")
        
        return blockers
    
    def _extract_select_columns(self, sql: str) -> Set[str]:
        """Extract column names from SELECT statement."""
        if not sql:
            return set()
        columns = set()
        
        # Pattern to match: column_name AS alias or just column_name
        patterns = [
            r'(\w+)\s+AS\s+(\w+)',  # column AS alias
            r'SELECT\s+([^FROM]+)',  # Everything between SELECT and FROM
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
            for match in matches:
                if len(match.groups()) == 2:
                    # Has alias
                    columns.add(match.group(2))
                else:
                    # Parse column list
                    column_list = match.group(1)
                    # Split by comma and extract column names
                    for col in column_list.split(','):
                        col = col.strip()
                        # Extract just the column name (handle schemas, functions, etc.)
                        col_match = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*$', col)
                        if col_match:
                            columns.add(col_match.group(1))
        
        return columns


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VOLUME ANALYZER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class VolumeAnalyzer:
    """Analyzes code changes for potential volume anomalies."""
    
    @staticmethod
    def detect_cartesian_product_risk(old_code: str, new_code: str) -> List[TechnicalBlocker]:
        """
        Detect potential Cartesian product from JOIN changes.
        
        Args:
            old_code: Previous SQL
            new_code: New SQL
        
        Returns:
            List of volume anomaly blockers
        """
        blockers = []
        
        # Extract JOINs
        old_joins = VolumeAnalyzer._extract_joins(old_code)
        new_joins = VolumeAnalyzer._extract_joins(new_code)
        
        # Check for added JOINs without proper conditions
        for new_join in new_joins:
            # Check if JOIN condition looks suspicious
            condition = new_join.get('condition', '')
            
            # Red flags:
            # 1. Missing ON condition
            # 2. Always-true condition (1=1)
            # 3. Cross join
            if not condition or '1=1' in condition or new_join.get('type', '').upper() == 'CROSS':
                blockers.append(TechnicalBlocker(
                    severity='HIGH',
                    blocker_type='volume_anomaly',
                    message=f"Potential Cartesian product: JOIN on {new_join.get('table', 'unknown')} may cause massive row explosion",
                    details={
                        'join_table': new_join.get('table'),
                        'join_condition': condition,
                        'estimated_impact': 'Could multiply row count by table size'
                    }
                ))
        
        return blockers
    
    @staticmethod
    def detect_filter_changes(old_code: str, new_code: str) -> List[TechnicalBlocker]:
        """
        Detect WHERE clause changes that could cause volume issues.
        
        Args:
            old_code: Previous SQL
            new_code: New SQL
        
        Returns:
            List of volume anomaly blockers
        """
        blockers = []
        
        old_where = VolumeAnalyzer._extract_where_clause(old_code)
        new_where = VolumeAnalyzer._extract_where_clause(new_code)
        
        # Detect filter removal (old had WHERE, new doesn't)
        if old_where and not new_where:
            blockers.append(TechnicalBlocker(
                severity='MEDIUM',
                blocker_type='volume_anomaly',
                message="WHERE clause removed - potential data explosion (full table scan)",
                details={
                    'old_filter': old_where,
                    'new_filter': None,
                    'estimated_impact': 'Row count may increase significantly'
                }
            ))
        
        # Detect overly restrictive filter (potential empty result)
        elif new_where and VolumeAnalyzer._is_overly_restrictive(new_where):
            blockers.append(TechnicalBlocker(
                severity='LOW',
                blocker_type='volume_anomaly',
                message="New WHERE clause may be too restrictive - risk of empty result set",
                details={
                    'new_filter': new_where,
                    'estimated_impact': 'May result in zero rows'
                }
            ))
        
        return blockers
    
    @staticmethod
    def _extract_joins(sql: str) -> List[Dict[str, str]]:
        """Extract JOIN information from SQL."""
        if not sql:
            return []
        joins = []
        pattern = r'(CROSS\s+JOIN|(?:LEFT|RIGHT|INNER|OUTER)?\s*JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:ON\s+(.*?))?(?:WHERE|GROUP|ORDER|LIMIT|JOIN|;|\Z)'
        
        for match in re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL):
            joins.append({
                'type': match.group(1).strip(),
                'table': match.group(2).strip(),
                'condition': match.group(3).strip() if match.group(3) else ''
            })
        
        return joins
    
    @staticmethod
    def _extract_where_clause(sql: str) -> Optional[str]:
        """Extract WHERE clause from SQL."""
        if not sql:
            return None
        match = re.search(
            r'WHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|;|\Z)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        return match.group(1).strip() if match else None
    
    @staticmethod
    def _is_overly_restrictive(where_clause: str) -> bool:
        """Check if WHERE clause is potentially too restrictive."""
        # Look for patterns like: column = NULL, impossible conditions, etc.
        restrictive_patterns = [
            r'=\s*NULL',  # Should use IS NULL
            r'AND.*AND.*AND',  # Many AND conditions
            r'<\s*0',  # Negative values where unlikely
        ]
        
        for pattern in restrictive_patterns:
            if re.search(pattern, where_clause, re.IGNORECASE):
                return True
        
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TECHNICAL VALIDATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TechnicalValidator:
    """
    Validates code changes for technical failures.
    
    Detects:
    - Type conflicts
    - Schema drift
    - Volume anomalies
    """
    
    def __init__(self, debug_engine: Optional[DebugEngine] = None):
        """
        Initialize validator.
        
        Args:
            debug_engine: Optional DebugEngine instance
        """
        self.engine = debug_engine
        if not self.engine:
            try:
                self.engine = DebugEngine()
            except Exception as e:
                logger.error(f"Could not initialize DebugEngine: {e}")
                self.engine = None
        
        self.schema_analyzer = SchemaAnalyzer(self.engine)
        self.volume_analyzer = VolumeAnalyzer()
    
    def validate(
        self,
        modified_elements: List[str],
        impacted_nodes: List[str],
        old_code: str,
        new_code: str
    ) -> TechnicalValidationReport:
        """
        Perform technical validation.
        
        Args:
            modified_elements: List of modified tables/columns
            impacted_nodes: List of impacted downstream tables
            old_code: Previous code version
            new_code: New code version
        
        Returns:
            TechnicalValidationReport
        """
        blockers = []
        
        # Step 0: SQL Syntax Validation (catches typos immediately!)
        syntax_errors = self._check_sql_syntax(new_code)
        for error_msg in syntax_errors:
            blockers.append(TechnicalBlocker(
                severity='HIGH',
                blocker_type='syntax_error',
                message=error_msg,
                details={'error_type': 'SQL Syntax Error'}
            ))
        
        # If we have syntax errors, return immediately
        if syntax_errors:
            return TechnicalValidationReport(
                technical_blockers=[b.to_dict() for b in blockers],
                has_high_severity=True,
                has_medium_severity=False,
                risk_level='HIGH',
                summary=f'SQL syntax errors detected: {len(syntax_errors)} error(s)'
            )
        
        # Extract table and columns from modified elements
        modified_table, modified_columns = self._parse_modified_elements(modified_elements)
        
        # 1. Detect type conflicts
        if modified_table and modified_columns:
            type_blockers = self.schema_analyzer.detect_type_conflicts(
                modified_table,
                modified_columns,
                impacted_nodes
            )
            blockers.extend(type_blockers)
        
        # 2. Detect schema drift
        schema_blockers = self.schema_analyzer.detect_schema_drift(
            old_code,
            new_code,
            impacted_nodes
        )
        blockers.extend(schema_blockers)
        
        # 3. Detect volume anomalies
        cartesian_blockers = self.volume_analyzer.detect_cartesian_product_risk(
            old_code,
            new_code
        )
        blockers.extend(cartesian_blockers)
        
        filter_blockers = self.volume_analyzer.detect_filter_changes(
            old_code,
            new_code
        )
        blockers.extend(filter_blockers)
        
        # Calculate overall risk
        has_high = any(b.severity == 'HIGH' for b in blockers)
        has_medium = any(b.severity == 'MEDIUM' for b in blockers)
        
        if has_high:
            risk_level = 'HIGH'
        elif has_medium:
            risk_level = 'MEDIUM'
        elif blockers:
            risk_level = 'LOW'
        else:
            risk_level = 'NONE'
        
        # Generate summary
        summary = self._generate_summary(blockers)
        
        return TechnicalValidationReport(
            technical_blockers=[b.to_dict() for b in blockers],
            has_high_severity=has_high,
            has_medium_severity=has_medium,
            risk_level=risk_level,
            summary=summary
        )
    
    def _parse_modified_elements(self, elements: List[str]) -> tuple:
        """Parse modified elements to extract table and columns."""
        if not elements:
            return None, []
        
        # Extract table from first element
        first = elements[0]
        parts = first.split('.')
        
        if len(parts) >= 2:
            table = '.'.join(parts[:-1])  # Everything except last part
            columns = [parts[-1]]  # Last part is column
            
            # Add more columns from other elements
            for elem in elements[1:]:
                elem_parts = elem.split('.')
                if len(elem_parts) >= 2:
                    columns.append(elem_parts[-1])
            
            return table, columns
        else:
            # Just a table name
            return first, []
    
    def _generate_summary(self, blockers: List[TechnicalBlocker]) -> str:
        """Generate human-readable summary."""
        if not blockers:
            return "No technical blockers detected"
        
        summary_parts = []
        
        # Count by severity
        high_count = sum(1 for b in blockers if b.severity == 'HIGH')
        medium_count = sum(1 for b in blockers if b.severity == 'MEDIUM')
        low_count = sum(1 for b in blockers if b.severity == 'LOW')
        
        if high_count:
            summary_parts.append(f"{high_count} HIGH severity issue(s)")
        if medium_count:
            summary_parts.append(f"{medium_count} MEDIUM severity issue(s)")
        if low_count:
            summary_parts.append(f"{low_count} LOW severity issue(s)")
        
        return ", ".join(summary_parts) + " detected"
    
    def _check_sql_syntax(self, sql_code: str) -> List[str]:
        """
        Check SQL code for syntax errors.
        
        Returns:
            List of error messages (empty if valid)
        """
        if not sql_code or not sql_code.strip():
            return []
        
        errors = []
        sql_upper = sql_code.upper()
        
        # Check for common SQL keyword typos
        typo_map = {
            'TABL ': 'TABLE',
            'SELCT ': 'SELECT',
            'CREAT ': 'CREATE',
            'WHER ': 'WHERE',
            'FRON ': 'FROM',
            'JOUN ': 'JOIN',
            'GROPU ': 'GROUP',
            'ODER ': 'ORDER',
            'SCHE ': 'SCHEMA',
        }
        
        for typo, correct in typo_map.items():
            if typo in sql_upper:
                # Find line number
                lines = sql_code.split('\n')
                for i, line in enumerate(lines, 1):
                    if typo.strip() in line.upper():
                        errors.append(
                            f"SQL Syntax Error at line {i}: Invalid keyword '{typo.strip()}' "
                            f"(did you mean '{correct}'?)"
                        )
                        break
        
        # Check for unmatched parentheses
        open_parens = sql_code.count('(')
        close_parens = sql_code.count(')')
        if open_parens != close_parens:
            errors.append(
                f"Unmatched parentheses: {open_parens} opening, {close_parens} closing"
            )
        
        return errors


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI / Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    
    # Test with sample SQL
    old_sql = """
    CREATE TABLE silver.orders AS
    SELECT
        order_id,
        customer_id,
        total_amount,
        status
    FROM raw.orders
    WHERE status = 'active'
    """
    
    new_sql = """
    CREATE TABLE silver.orders AS
    SELECT
        order_id,
        customer_id::STRING as customer_id,
        total_amount * 1.1 as total_amount
    FROM raw.orders
    CROSS JOIN raw.products
    """
    
    validator = TechnicalValidator()
    report = validator.validate(
        modified_elements=['silver.orders.customer_id', 'silver.orders.total_amount'],
        impacted_nodes=['gold.revenue_report'],
        old_code=old_sql,
        new_code=new_sql
    )
    
    print("\n" + "="*70)
    print("TECHNICAL VALIDATION RESULTS")
    print("="*70)
    print(json.dumps(report.to_dict(), indent=2))
