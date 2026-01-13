"""
╔══════════════════════════════════════════════════════════════════╗
║                      debug_engine.py                              ║
║                  🔍 GENERIC Debug Engine                          ║
╠══════════════════════════════════════════════════════════════════╣
║  CLIENT-AGNOSTIC: Works with ANY database schema!                 ║
║                                                                   ║
║  • No hardcoded table names                                       ║
║  • No hardcoded schema names                                      ║
║  • Auto-discovers available tables                                ║
║  • Works with any metadata structure                              ║
╚══════════════════════════════════════════════════════════════════╝

USAGE:
    from debug_engine import DebugEngine

    engine = DebugEngine('/path/to/database.duckdb')

    # Discover what's available
    tables = engine.list_tables()

    # Trace column lineage
    report = engine.trace_column_lineage('schema.table', 'column')
"""

import re
import logging
from pathlib import Path
from contextlib import contextmanager
from functools import lru_cache
from typing import Dict, List, Optional, Any, Generator
from abc import ABC, abstractmethod

import duckdb
import pandas as pd


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('DebugEngine')


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECURITY: Input Validation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Valid identifier pattern: letters, numbers, underscores
# Optional schema prefix: schema.table
SAFE_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$')


def validate_identifier(name: str, identifier_type: str = 'identifier') -> str:
    """
    Validate a SQL identifier (table name, column name, etc.)

    Prevents SQL injection attacks.

    Args:
        name: The identifier to validate
        identifier_type: What kind of identifier (for error messages)

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier is invalid
    """
    if not name or not SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'\n"
            f"Only letters, numbers, and underscores allowed."
        )
    return name


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ABSTRACT BASE: Database Connector
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DatabaseConnector(ABC):
    """
    Abstract base class for database connections.

    Extend this class to support new database types
    (Snowflake, Databricks, PostgreSQL, etc.)
    """

    @abstractmethod
    def execute(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        pass

    @abstractmethod
    def get_schemas(self) -> List[str]:
        """Get list of available schemas."""
        pass

    @abstractmethod
    def get_tables(self, schema: Optional[str] = None) -> List[Dict[str, str]]:
        """Get list of tables, optionally filtered by schema."""
        pass


class DuckDBConnector(DatabaseConnector):
    """DuckDB implementation of DatabaseConnector."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def _connect(self, read_only: bool = True) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Context manager for safe connections."""
        con = duckdb.connect(self.db_path, read_only=read_only)
        try:
            yield con
        finally:
            con.close()

    def execute(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        with self._connect() as con:
            if params:
                return con.execute(query, params).fetchdf()
            return con.execute(query).fetchdf()

    def get_schemas(self) -> List[str]:
        """Get all user schemas (excluding system schemas)."""
        query = """
            SELECT DISTINCT table_schema 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema
        """
        df = self.execute(query)
        return df['table_schema'].tolist()

    def get_tables(self, schema: Optional[str] = None) -> List[Dict[str, str]]:
        """Get all tables, optionally filtered by schema."""
        if schema:
            validate_identifier(schema, 'schema')
            query = """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                ORDER BY table_name
            """
            df = self.execute(query, [schema])
        else:
            query = """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """
            df = self.execute(query)

        return df.to_dict(orient='records')


class DatabricksConnector(DatabaseConnector):
    """Databricks implementation of DatabaseConnector."""

    def __init__(self, config: Dict[str, str]):
        self.host = config['host']
        self.token = config['token']
        self.http_path = config['http_path']
        self.catalog = config.get('catalog', 'hive_metastore')
        self._connection = None
        self._cursor = None

    def _get_connection(self):
        """Get or create a persistent connection."""
        if self._connection is None:
            from databricks import sql
            self._connection = sql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token,
                catalog=self.catalog
            )
            self._cursor = self._connection.cursor()
        return self._connection, self._cursor

    def execute(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        _, cursor = self._get_connection()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return pd.DataFrame(result, columns=columns)

    def get_schemas(self) -> List[str]:
        """Get all schemas in the current catalog."""
        query = f"""
            SELECT DISTINCT schema_name
            FROM {self.catalog}.information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', '__databricks_internal')
            ORDER BY schema_name
        """
        df = self.execute(query)
        return df['schema_name'].tolist()

    def get_tables(self, schema: Optional[str] = None) -> List[Dict[str, str]]:
        """Get tables, optionally filtered by schema."""
        if schema:
            query = f"""
                SELECT table_schema, table_name
                FROM {self.catalog}.information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """
        else:
            query = f"""
                SELECT table_schema, table_name
                FROM {self.catalog}.information_schema.tables
                WHERE table_schema NOT IN ('information_schema', '__databricks_internal')
                ORDER BY table_schema, table_name
            """
        df = self.execute(query)
        return df.to_dict(orient='records')

    def close(self):
        """Close connection."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN CLASS: Debug Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DebugEngine:
    """
    🔍 Generic Data Debug Engine

    Works with ANY database schema. No hardcoded values!

    Features:
    - Auto-discovers tables and schemas
    - Traces column lineage
    - Debugs data quality issues
    - Works with any metadata structure

    Usage:
        engine = DebugEngine('/path/to/db.duckdb')

        # Discovery
        tables = engine.list_tables()
        schemas = engine.list_schemas()

        # Lineage
        report = engine.trace_column_lineage('schema.table', 'column')

        # Debugging
        stats = engine.check_table_sources('schema.table')
    """

    # Default metadata table names (can be customized per client)
    DEFAULT_TABLE_LINEAGE = 'meta.table_lineage'
    DEFAULT_COLUMN_LINEAGE = 'meta.column_lineage'

    def __init__(
        self,
        db_path: Optional[str] = None,
        db_type: Optional[str] = None,
        table_lineage_table: Optional[str] = None,
        column_lineage_table: Optional[str] = None
    ):
        """
        Initialize the Debug Engine.

        Args:
            db_path: Path to database (for duckdb). If None, reads from config.
            db_type: Database type. If None, reads from config.yml
            table_lineage_table: Custom metadata table for table lineage
            column_lineage_table: Custom metadata table for column lineage
        """
        # Load from config if not provided
        from config.db_config import get_db_type, get_duckdb_config, get_databricks_config
        
        self.db_type = db_type or get_db_type()
        
        # Initialize connector based on type
        if self.db_type == 'duckdb':
            if db_path is None:
                config = get_duckdb_config()
                db_path = config['path']
            
            self.db_path = db_path
            self.connector = DuckDBConnector(db_path)
            logger.info(f"DebugEngine initialized: DuckDB at {db_path}")
            
        elif self.db_type == 'databricks':
            config = get_databricks_config()
            self.connector = DatabricksConnector(config)
            self.db_path = f"databricks://{config['host']}"
            logger.info(f"DebugEngine initialized: Databricks at {config['host']}")
            
        else:
            raise NotImplementedError(f"Database type '{self.db_type}' not yet supported")

        # Metadata table names
        self.table_lineage_table = table_lineage_table or self.DEFAULT_TABLE_LINEAGE
        self.column_lineage_table = column_lineage_table or self.DEFAULT_COLUMN_LINEAGE

    # ─────────────────────────────────────────────────────────────
    # DISCOVERY METHODS
    # ─────────────────────────────────────────────────────────────

    def list_schemas(self) -> List[str]:
        """
        📁 List all available schemas in the database.

        Returns:
            List of schema names
        """
        return self.connector.get_schemas()

    def list_tables(self, schema: Optional[str] = None) -> List[Dict[str, str]]:
        """
        📋 List all tables in the database.

        Args:
            schema: Optional - filter to specific schema

        Returns:
            List of dicts with 'table_schema' and 'table_name'
        """
        return self.connector.get_tables(schema)

    def describe_table(self, table: str) -> List[Dict[str, Any]]:
        """
        📊 Get column information for a table.

        Args:
            table: Full table name (e.g., 'raw.employees')

        Returns:
            List of column definitions
        """
        validate_identifier(table, 'table')
        df = self.connector.execute(f"DESCRIBE {table}")
        return df.to_dict(orient='records')

    def get_row_count(self, table: str) -> int:
        """
        🔢 Get row count for a table.

        Args:
            table: Full table name

        Returns:
            Number of rows
        """
        validate_identifier(table, 'table')
        df = self.connector.execute(f"SELECT COUNT(*) as cnt FROM {table}")
        return int(df['cnt'].iloc[0])

    # ─────────────────────────────────────────────────────────────
    # LINEAGE METHODS
    # ─────────────────────────────────────────────────────────────

    def _check_metadata_exists(self) -> Dict[str, bool]:
        """Check if metadata tables exist."""
        tables = self.list_tables('meta')
        table_names = [f"meta.{t['table_name']}" for t in tables]

        return {
            'table_lineage': self.table_lineage_table in table_names,
            'column_lineage': self.column_lineage_table in table_names
        }

    @lru_cache(maxsize=100)
    def trace_column_lineage(self, target_table: str, target_column: str) -> str:
        """
        🔍 Trace how a column is calculated.

        Args:
            target_table: Table containing the column (e.g., 'conformed.churn_risk')
            target_column: Column to trace (e.g., 'risk_level')

        Returns:
            Formatted report explaining the column's lineage
        """
        validate_identifier(target_table, 'table')
        validate_identifier(target_column, 'column')

        # Check metadata exists
        meta_status = self._check_metadata_exists()
        if not meta_status['column_lineage']:
            return (
                f"❌ Metadata table not found: {self.column_lineage_table}\n"
                f"   Run build_metadata.py first to create lineage data."
            )

        # Query the metadata
        query = f"""
            SELECT *
            FROM {self.column_lineage_table}
            WHERE target_table = ? AND target_column = ?
        """

        try:
            df = self.connector.execute(query, [target_table, target_column])
        except Exception as e:
            return f"❌ Query error: {e}"

        if df.empty:
            return (
                f"❌ No lineage found for: {target_table}.{target_column}\n"
                f"\n"
                f"Possible reasons:\n"
                f"  • Column doesn't exist in metadata\n"
                f"  • Run build_metadata.py to refresh\n"
                f"  • Column might be a simple pass-through"
            )

        # Build report (generic - works with any column structure)
        row = df.iloc[0]

        report_lines = [
            "",
            "╔═══════════════════════════════════════════════════════════════╗",
            "║  🔍 COLUMN LINEAGE REPORT                                      ║",
            "╠═══════════════════════════════════════════════════════════════╣",
            f"║  📍 Target: {target_table}.{target_column}",
            "║",
        ]

        # Add all available fields from the row (GENERIC!)
        for col in df.columns:
            if col not in ['target_table', 'target_column']:
                value = row[col]
                if value and str(value).strip():
                    # Format multi-line values
                    if '\n' in str(value):
                        report_lines.append(f"║  📦 {col}:")
                        for line in str(value).split('\n'):
                            report_lines.append(f"║     {line}")
                    else:
                        report_lines.append(f"║  📦 {col}: {value}")

        report_lines.extend([
            "║",
            "╚═══════════════════════════════════════════════════════════════╝",
            ""
        ])

        return '\n'.join(report_lines)

    def get_upstream_tables(self, target_table: str) -> List[str]:
        """
        📥 Get tables that feed into the target table.

        Args:
            target_table: Table to investigate

        Returns:
            List of source table names
        """
        validate_identifier(target_table, 'table')

        meta_status = self._check_metadata_exists()
        if not meta_status['table_lineage']:
            logger.warning(f"Metadata table not found: {self.table_lineage_table}")
            return []

        query = f"""
            SELECT DISTINCT source_table
            FROM {self.table_lineage_table}
            WHERE target_table = ?
        """

        try:
            df = self.connector.execute(query, [target_table])
            return df['source_table'].tolist()
        except Exception as e:
            logger.error(f"Error getting upstream tables: {e}")
            return []

    def get_lineage_tree(self, target_table: str, max_depth: int = 5) -> Dict[str, Any]:
        """
        🌳 Get full lineage tree (recursive).

        Args:
            target_table: Starting point
            max_depth: Maximum recursion depth

        Returns:
            Nested dictionary of lineage
        """
        validate_identifier(target_table, 'table')

        if max_depth <= 0:
            return {"_truncated": True}

        upstream = self.get_upstream_tables(target_table)

        if not upstream:
            return {"_is_source": True}

        tree = {}
        for source in upstream:
            tree[source] = self.get_lineage_tree(source, max_depth - 1)

        return tree

    # ─────────────────────────────────────────────────────────────
    # DEBUGGING METHODS
    # ─────────────────────────────────────────────────────────────

    def check_table_sources(self, target_table: str) -> Dict[str, Any]:
        """
        🔬 Check health of source tables.

        Args:
            target_table: Table to debug

        Returns:
            Dict with stats for each source table
        """
        validate_identifier(target_table, 'table')

        upstream = self.get_upstream_tables(target_table)

        if not upstream:
            return {
                "error": f"No upstream tables found for {target_table}",
                "hint": "Check if lineage metadata exists"
            }

        results = {}
        for source in upstream:
            try:
                count = self.get_row_count(source)
                results[source] = {
                    "row_count": count,
                    "status": "✅" if count > 0 else "⚠️ EMPTY"
                }
            except Exception as e:
                results[source] = {
                    "error": str(e),
                    "status": "❌ ERROR"
                }

        return results

    def inspect_row(self, table: str, key_column: str, key_value: Any) -> Dict[str, Any]:
        """
        🔎 Fetch a specific row from a table.

        Args:
            table: Table to query
            key_column: Column to filter by
            key_value: Value to look for

        Returns:
            Row data as dictionary
        """
        validate_identifier(table, 'table')
        validate_identifier(key_column, 'column')

        # Safe query with parameter
        query = f"SELECT * FROM {table} WHERE {key_column} = ?"

        try:
            df = self.connector.execute(query, [key_value])

            if df.empty:
                return {
                    "status": "not_found",
                    "message": f"No row where {key_column} = {key_value}"
                }

            return {"row": df.to_dict(orient='records')[0]}

        except Exception as e:
            return {"error": str(e)}

    def detect_duplicates(self, table: str, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        🔍 Detect duplicate rows in a table.

        If columns not specified, checks all columns for exact duplicates.
        If columns specified, checks for duplicates based on those columns.

        Args:
            table: Table to check
            columns: Optional list of columns to check. If None, checks all columns.

        Returns:
            Dict with duplicate information and explanation
        """
        validate_identifier(table, 'table')

        try:
            # Get table structure
            table_info = self.describe_table(table)
            all_columns = [col['column_name'] for col in table_info]

            if columns:
                # Validate specified columns exist
                for col in columns:
                    validate_identifier(col, 'column')
                    if col not in all_columns:
                        return {"error": f"Column '{col}' not found in table {table}"}
                check_columns = columns
            else:
                check_columns = all_columns

            if not check_columns:
                return {"error": f"Table {table} has no columns to check"}

            # Build safe column list
            cols_str = ', '.join(check_columns)
            
            # Query to find duplicates
            duplicate_query = f"""
                SELECT {cols_str}, COUNT(*) as duplicate_count
                FROM {table}
                GROUP BY {cols_str}
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
            """

            duplicates_df = self.connector.execute(duplicate_query)

            if duplicates_df.empty:
                return {
                    "has_duplicates": False,
                    "table": table,
                    "columns_checked": check_columns,
                    "message": f"No duplicates found in {table}"
                }

            # Get total duplicate rows count
            total_duplicate_rows = duplicates_df['duplicate_count'].sum() - len(duplicates_df)

            # Get sample duplicate records
            duplicate_groups = duplicates_df.head(10).to_dict(orient='records')

            # Generate explanation
            explanation = self._explain_duplicates(table, check_columns, duplicate_groups, total_duplicate_rows)

            return {
                "has_duplicates": True,
                "table": table,
                "columns_checked": check_columns,
                "duplicate_groups_count": len(duplicates_df),
                "total_duplicate_rows": int(total_duplicate_rows),
                "duplicate_groups": duplicate_groups,
                "explanation": explanation,
                "recommendation": self._suggest_duplicate_fix(table, check_columns)
            }

        except Exception as e:
            return {"error": str(e)}

    def validate_business_rules(self, table: str, rules: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        ✅ Validate data against business rules.

        Args:
            table: Table to validate
            rules: List of SQL WHERE clause conditions. If None, auto-detects common rules.

        Returns:
            Dict with validation results for each rule
        """
        validate_identifier(table, 'table')

        try:
            if not rules:
                # Auto-detect common business rules based on column names and types
                rules = self._auto_detect_rules(table)

            if not rules:
                return {
                    "error": "No business rules found to validate",
                    "hint": "Specify rules manually or ensure table has columns with standard naming patterns"
                }

            table_info = self.describe_table(table)
            results = []

            for rule in rules:
                # Validate rule doesn't contain dangerous operations
                if any(dangerous in rule.upper() for dangerous in ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE']):
                    results.append({
                        "rule": rule,
                        "status": "SKIPPED",
                        "reason": "Rule contains dangerous SQL operations"
                    })
                    continue

                try:
                    # Find violations (rows where rule is FALSE)
                    violation_query = f"SELECT * FROM {table} WHERE NOT ({rule}) LIMIT 10"
                    violations_df = self.connector.execute(violation_query)

                    total_count_query = f"SELECT COUNT(*) as total FROM {table} WHERE NOT ({rule})"
                    total_violations = self.connector.execute(total_count_query)

                    violation_count = int(total_violations.iloc[0]['total']) if not total_violations.empty else 0

                    results.append({
                        "rule": rule,
                        "status": "PASS" if violation_count == 0 else "FAIL",
                        "violation_count": violation_count,
                        "violations_sample": violations_df.head(5).to_dict(orient='records') if not violations_df.empty else []
                    })

                except Exception as e:
                    results.append({
                        "rule": rule,
                        "status": "ERROR",
                        "error": str(e)
                    })

            return {
                "table": table,
                "validations": results,
                "summary": {
                    "total_rules": len(results),
                    "passed": sum(1 for r in results if r.get("status") == "PASS"),
                    "failed": sum(1 for r in results if r.get("status") == "FAIL"),
                    "errors": sum(1 for r in results if r.get("status") == "ERROR")
                }
            }

        except Exception as e:
            return {"error": str(e)}

    def analyze_data_quality(self, table: str) -> Dict[str, Any]:
        """
        📊 Comprehensive data quality analysis.

        Args:
            table: Table to analyze

        Returns:
            Dict with data quality metrics and recommendations
        """
        validate_identifier(table, 'table')

        try:
            table_info = self.describe_table(table)
            columns = [col['column_name'] for col in table_info]

            # Get row count
            row_count = self.get_row_count(table)

            if row_count == 0:
                return {
                    "table": table,
                    "row_count": 0,
                    "status": "EMPTY",
                    "message": "Table is empty, cannot analyze data quality"
                }

            # Analyze each column
            column_stats = {}
            null_analysis = {}
            type_issues = []

            for col_info in table_info:
                col_name = col_info['column_name']
                col_type = col_info['column_type'].upper()

                # Count nulls
                null_query = f"SELECT COUNT(*) as null_count FROM {table} WHERE {col_name} IS NULL"
                null_result = self.connector.execute(null_query)
                null_count = int(null_result.iloc[0]['null_count']) if not null_result.empty else 0
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0

                null_analysis[col_name] = {
                    "null_count": null_count,
                    "null_percentage": round(null_pct, 2),
                    "status": "OK" if null_pct < 5 else "WARNING" if null_pct < 50 else "CRITICAL"
                }

                # Check for type issues (string dates, etc.)
                if col_type == 'VARCHAR':
                    # Check if it looks like a date
                    if 'date' in col_name.lower():
                        type_issues.append({
                            "column": col_name,
                            "issue": f"Column '{col_name}' is VARCHAR but contains date values",
                            "recommendation": "Consider converting to DATE type for better validation and query performance"
                        })

                # For numeric columns, check for outliers
                if col_type in ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC']:
                    try:
                        stats_query = f"""
                            SELECT 
                                MIN({col_name}) as min_val,
                                MAX({col_name}) as max_val,
                                AVG({col_name}) as avg_val,
                                COUNT(DISTINCT {col_name}) as distinct_count
                            FROM {table}
                            WHERE {col_name} IS NOT NULL
                        """
                        stats = self.connector.execute(stats_query)
                        if not stats.empty and stats.iloc[0]['min_val'] is not None:
                            column_stats[col_name] = {
                                "min": float(stats.iloc[0]['min_val']),
                                "max": float(stats.iloc[0]['max_val']),
                                "avg": float(stats.iloc[0]['avg_val']),
                                "distinct_values": int(stats.iloc[0]['distinct_count'])
                            }
                    except:
                        pass

            # Check for potential duplicates
            duplicate_check = self.detect_duplicates(table, None)
            has_duplicates = duplicate_check.get("has_duplicates", False)

            # Generate recommendations
            recommendations = []
            if has_duplicates:
                recommendations.append("Duplicate rows detected. Review duplicate detection results.")
            
            for issue in type_issues:
                recommendations.append(issue["recommendation"])

            high_null_cols = [col for col, stats in null_analysis.items() if stats["status"] == "CRITICAL"]
            if high_null_cols:
                recommendations.append(f"Columns with >50% nulls: {', '.join(high_null_cols)}. Consider data validation or default values.")

            return {
                "table": table,
                "row_count": row_count,
                "columns_analyzed": len(columns),
                "null_analysis": null_analysis,
                "numeric_statistics": column_stats,
                "type_issues": type_issues,
                "duplicates_detected": has_duplicates,
                "recommendations": recommendations,
                "quality_score": self._calculate_quality_score(null_analysis, has_duplicates, type_issues)
            }

        except Exception as e:
            return {"error": str(e)}

    def _explain_duplicates(self, table: str, columns: List[str], duplicate_groups: List[Dict], total_duplicate_rows: int) -> str:
        """Generate explanation for why duplicates exist."""
        explanation = f"Found {len(duplicate_groups)} duplicate group(s) with {total_duplicate_rows} total duplicate rows in {table}.\n\n"
        
        if len(duplicate_groups) <= 3:
            explanation += "Duplicate groups:\n"
            for group in duplicate_groups:
                # Make a copy to avoid mutating the original
                group_copy = dict(group)
                dup_count = group_copy.pop('duplicate_count', 'N/A')
                key_values = ', '.join([f"{k}={v}" for k, v in group_copy.items()])
                explanation += f"- {key_values} appears {dup_count} times\n"
        
        explanation += f"\nPossible reasons:\n"
        explanation += "- Missing DISTINCT in source SQL transformation\n"
        explanation += "- JOIN conditions creating cartesian products\n"
        explanation += "- Data entry errors (true duplicates)\n"
        explanation += "- Missing unique constraints\n"
        
        # Check if this is a fact table that might legitimately have duplicates
        if 'fact' in table.lower():
            explanation += "- Note: Fact tables may legitimately have multiple rows for the same dimensions (e.g., multiple transactions)\n"
        
        return explanation

    def _suggest_duplicate_fix(self, table: str, columns: List[str]) -> str:
        """Suggest how to fix duplicates."""
        suggestion = f"To fix duplicates in {table}:\n"
        suggestion += f"1. Review the SQL that creates {table} - add DISTINCT if duplicates shouldn't exist\n"
        suggestion += f"2. Check JOIN conditions - ensure proper keys are used\n"
        
        if len(columns) <= 3:
            suggestion += f"3. Consider adding a UNIQUE constraint on ({', '.join(columns)}) if these columns should uniquely identify rows\n"
        else:
            suggestion += f"3. Consider adding a UNIQUE constraint on key columns if duplicates are invalid\n"
        
        suggestion += f"4. Use ROW_NUMBER() with PARTITION BY to deduplicate if needed\n"
        
        return suggestion

    def _auto_detect_rules(self, table: str) -> List[str]:
        """Auto-detect common business rules based on column names and types."""
        table_info = self.describe_table(table)
        rules = []

        for col_info in table_info:
            col_name = col_info['column_name']
            col_type = col_info['column_type'].upper()
            col_lower = col_name.lower()

            # Date rules
            if 'start_date' in col_lower and 'end_date' in [c['column_name'].lower() for c in table_info]:
                end_col = next((c['column_name'] for c in table_info if 'end_date' in c['column_name'].lower()), None)
                if end_col:
                    rules.append(f"{end_col} >= {col_name} OR {end_col} IS NULL")
                    rules.append(f"{col_name} IS NOT NULL")

            # Positive number rules
            if col_type in ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC']:
                if any(keyword in col_lower for keyword in ['salary', 'price', 'amount', 'cost', 'revenue', 'quantity']):
                    rules.append(f"{col_name} >= 0")
                if 'id' in col_lower:
                    rules.append(f"{col_name} > 0")

            # Not null rules for key columns
            if 'id' in col_lower or col_name.endswith('_id'):
                rules.append(f"{col_name} IS NOT NULL")

            # Enum/string validation
            if 'status' in col_lower or 'state' in col_lower or 'level' in col_lower or 'type' in col_lower:
                # Don't auto-generate rules for these, would need to know valid values
                pass

        return list(set(rules))  # Remove duplicates

    def _calculate_quality_score(self, null_analysis: Dict, has_duplicates: bool, type_issues: List) -> Dict[str, Any]:
        """Calculate overall data quality score."""
        score = 100
        issues = []

        # Deduct for nulls
        critical_nulls = sum(1 for stats in null_analysis.values() if stats["status"] == "CRITICAL")
        warning_nulls = sum(1 for stats in null_analysis.values() if stats["status"] == "WARNING")
        
        score -= critical_nulls * 20
        score -= warning_nulls * 5

        # Deduct for duplicates
        if has_duplicates:
            score -= 15
            issues.append("Duplicates detected")

        # Deduct for type issues
        score -= len(type_issues) * 5

        score = max(0, score)  # Don't go below 0

        quality_level = "EXCELLENT" if score >= 90 else "GOOD" if score >= 70 else "FAIR" if score >= 50 else "POOR"

        return {
            "score": score,
            "level": quality_level,
            "issues_count": critical_nulls + warning_nulls + (1 if has_duplicates else 0) + len(type_issues)
        }

    def clear_cache(self):
        """Clear cached lineage results."""
        self.trace_column_lineage.cache_clear()
        logger.info("Cache cleared")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONVENIENCE FUNCTIONS (for backwards compatibility)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Default engine instance (lazy loaded)
_default_engine: Optional[DebugEngine] = None


def get_engine() -> DebugEngine:
    """Get or create the default engine instance."""
    global _default_engine
    if _default_engine is None:
        _default_engine = DebugEngine()
    return _default_engine


def trace_column_lineage(target_table: str, target_column: str) -> str:
    """Convenience function using default engine."""
    return get_engine().trace_column_lineage(target_table, target_column)


def debug_query_dependencies(target_table: str) -> Dict[str, Any]:
    """Convenience function using default engine."""
    return get_engine().check_table_sources(target_table)


def get_upstream_tables(target_table: str) -> List[str]:
    """Convenience function using default engine."""
    return get_engine().get_upstream_tables(target_table)


def get_lineage_tree(target_table: str) -> Dict[str, Any]:
    """Convenience function using default engine."""
    return get_engine().get_lineage_tree(target_table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SELF-TEST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    import sys
    import os

    # Add project root to path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    print()
    print('=' * 65)
    print('🔍 DEBUG AI ENGINE - Generic Test')
    print('=' * 65)

    # Initialize engine (auto-detects database)
    try:
        engine = DebugEngine()
        print(f"\n✅ Database: {engine.db_path}")
    except Exception as e:
        print(f"\n❌ Failed to initialize: {e}")
        sys.exit(1)

    # Test 1: List schemas (GENERIC - works for any DB)
    print('\n' + '-' * 65)
    print('TEST 1: Discover available schemas')
    print('-' * 65)
    schemas = engine.list_schemas()
    for s in schemas:
        print(f"   📁 {s}")

    # Test 2: List tables (GENERIC)
    print('\n' + '-' * 65)
    print('TEST 2: Discover available tables')
    print('-' * 65)
    tables = engine.list_tables()
    for t in tables:
        full_name = f"{t['table_schema']}.{t['table_name']}"
        try:
            count = engine.get_row_count(full_name)
            print(f"   📋 {full_name} ({count} rows)")
        except:
            print(f"   📋 {full_name}")

    # Test 3: Check metadata status
    print('\n' + '-' * 65)
    print('TEST 3: Check metadata status')
    print('-' * 65)
    meta_status = engine._check_metadata_exists()
    for name, exists in meta_status.items():
        status = '✅' if exists else '❌'
        print(f"   {status} {name}")

    # Test 4: Column lineage (if metadata exists)
    if meta_status['column_lineage']:
        print('\n' + '-' * 65)
        print('TEST 4: Trace column lineage')
        print('-' * 65)

        # Get a sample from metadata
        sample_query = f"SELECT target_table, target_column FROM {engine.column_lineage_table} LIMIT 1"
        try:
            sample = engine.connector.execute(sample_query)
            if not sample.empty:
                t_table = sample['target_table'].iloc[0]
                t_column = sample['target_column'].iloc[0]
                print(f"   Testing: {t_table}.{t_column}")
                report = engine.trace_column_lineage(t_table, t_column)
                print(report)
        except Exception as e:
            print(f"   ⚠️ Could not test: {e}")

    # Test 5: Lineage tree (if metadata exists)
    if meta_status['table_lineage']:
        print('\n' + '-' * 65)
        print('TEST 5: Lineage tree')
        print('-' * 65)

        # Get a sample target table
        sample_query = f"SELECT DISTINCT target_table FROM {engine.table_lineage_table} LIMIT 1"
        try:
            sample = engine.connector.execute(sample_query)
            if not sample.empty:
                t_table = sample['target_table'].iloc[0]
                print(f"   Testing: {t_table}")
                tree = engine.get_lineage_tree(t_table)
                print(json.dumps(tree, indent=2))
        except Exception as e:
            print(f"   ⚠️ Could not test: {e}")

    print('\n' + '=' * 65)
    print('✅ All tests completed!')
    print('=' * 65)
    print()