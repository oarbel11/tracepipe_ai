"""
╔══════════════════════════════════════════════════════════════════╗
║                      mcp_server.py                                ║
║                 🌐 GENERIC MCP Server                             ║
╠══════════════════════════════════════════════════════════════════╣
║  CLIENT-AGNOSTIC: Works with ANY database!                        ║
║                                                                   ║
║  Exposes the Debug Engine as an MCP server that AI agents         ║
║  (Claude, Cursor, etc.) can connect to.                           ║
║                                                                   ║
║  All tools are generic - no hardcoded table names or schemas!    ║
╚══════════════════════════════════════════════════════════════════╝

USAGE:
    python mcp_server.py

ENVIRONMENT VARIABLES:
    TRACEPIPE_AI_DB_PATH - Path to database (optional, auto-detects)
    TRACEPIPE_AI_DB_TYPE - Database type (default: duckdb)
    DEBUG_AI_* - Also supported for backward compatibility
"""

import os
import sys
from typing import Optional, List, Any

# Fix Windows console encoding for Unicode characters
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        # Python < 3.7 or reconfigure failed, use environment variable
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add project root and scripts folder to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'scripts')
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, SCRIPTS_DIR)

# Import our generic debug engine (now in scripts folder)
from debug_engine import DebugEngine, validate_identifier

# Import MCP
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("[ERROR] MCP library not installed!")
    print("   Install with: pip install mcp")
    sys.exit(1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MCP SERVER SETUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Create MCP server
mcp = FastMCP("DebugAI", json_response=True)

# Create debug engine instance (lazy loading)
_engine: Optional[DebugEngine] = None


def get_engine() -> DebugEngine:
    """Get or create the engine instance."""
    global _engine
    if _engine is None:
        _engine = DebugEngine()
    return _engine


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DISCOVERY TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def list_schemas() -> dict:
    """
    📁 List all available schemas in the database.

    Use this first to discover what data is available.

    Returns:
        List of schema names.

    Example:
        list_schemas()
        → {"schemas": ["raw", "silver", "gold", "meta"]}
    """
    try:
        schemas = get_engine().list_schemas()
        return {"schemas": schemas}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_tables(schema: Optional[str] = None) -> dict:
    """
    📋 List all tables, optionally filtered by schema.

    Args:
        schema: Optional schema name to filter by.
                If not provided, lists ALL tables.

    Returns:
        List of tables with schema and name.

    Example:
        list_tables()
        → {"tables": [{"table_schema": "raw", "table_name": "employees"}, ...]}

        list_tables(schema="raw")
        → Only tables in the 'raw' schema
    """
    try:
        tables = get_engine().list_tables(schema)
        return {"tables": tables}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def describe_table(table_name: str) -> dict:
    """
    📊 Get column information for a table.

    Args:
        table_name: Full table name (e.g., "raw.employees")

    Returns:
        List of columns with their data types.

    Example:
        describe_table("raw.employees")
        → {"columns": [{"column_name": "emp_id", "column_type": "INTEGER"}, ...]}
    """
    try:
        columns = get_engine().describe_table(table_name)
        return {"columns": columns}
    except ValueError as e:
        return {"error": f"Invalid table name: {e}"}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_row_count(table_name: str) -> dict:
    """
    🔢 Get the number of rows in a table.

    Args:
        table_name: Full table name (e.g., "raw.employees")

    Returns:
        Row count.

    Example:
        get_row_count("raw.employees")
        → {"table": "raw.employees", "row_count": 1000}
    """
    try:
        count = get_engine().get_row_count(table_name)
        return {"table": table_name, "row_count": count}
    except Exception as e:
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LINEAGE TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def explain_column(target_table: str, target_column: str) -> str:
    """
    🔍 Explain how a column is calculated (THE MAIN FEATURE!)

    Use this to answer questions like:
    - "Why is this employee marked as HIGH RISK?"
    - "How is the total_revenue calculated?"
    - "What logic determines the status field?"

    Args:
        target_table: Table containing the column (e.g., "conformed.churn_risk")
        target_column: Column to explain (e.g., "risk_level")

    Returns:
        Detailed report showing:
        - Source tables
        - Transformation logic (SQL)
        - File where it's defined

    Example:
        explain_column("conformed.churn_risk", "risk_level")
        → Shows the CASE WHEN logic that calculates risk
    """
    try:
        return get_engine().trace_column_lineage(target_table, target_column)
    except Exception as e:
        return f"❌ Error: {e}"


@mcp.tool()
def get_table_sources(target_table: str) -> dict:
    """
    📥 Get the upstream tables that feed into a target table.

    Args:
        target_table: Table to investigate (e.g., "silver.fact_jobs")

    Returns:
        List of source table names.

    Example:
        get_table_sources("silver.fact_jobs")
        → {"target": "silver.fact_jobs", "sources": ["raw.job_history", "raw.companies"]}
    """
    try:
        sources = get_engine().get_upstream_tables(target_table)
        return {"target": target_table, "sources": sources}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_lineage_tree(target_table: str) -> dict:
    """
    🌳 Get the complete lineage tree for a table.

    Traces ALL the way back to source data.

    Args:
        target_table: Starting point (e.g., "conformed.churn_risk")

    Returns:
        Nested tree showing all upstream dependencies.

    Example:
        get_lineage_tree("conformed.churn_risk")
        → {
            "lineage": {
                "silver.dim_employees": {
                    "raw.employees": {"_is_source": true}
                }
            }
          }
    """
    try:
        tree = get_engine().get_lineage_tree(target_table)
        return {"target": target_table, "lineage": tree}
    except Exception as e:
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DEBUGGING TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def check_table_health(target_table: str) -> dict:
    """
    🔬 Check the health of source tables (debugging tool).

    Use this when something looks wrong:
    - "Why is my result empty?"
    - "Why are there missing values?"

    Checks:
    - Row counts in all source tables
    - Whether any sources are empty

    Args:
        target_table: Table having issues (e.g., "silver.fact_jobs")

    Returns:
        Stats for each source table.

    Example:
        check_table_health("silver.fact_jobs")
        → {
            "raw.job_history": {"row_count": 100, "status": "✅"},
            "raw.companies": {"row_count": 0, "status": "⚠️ EMPTY"}
          }
    """
    try:
        return get_engine().check_table_sources(target_table)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def inspect_row(table: str, key_column: str, key_value: str) -> dict:
    """
    🔎 Fetch a specific row from a table.

    Use this to see actual data values.

    Args:
        table: Table to query (e.g., "raw.employees")
        key_column: Column to search by (e.g., "emp_id")
        key_value: Value to look for (e.g., "101")

    Returns:
        The row data as a dictionary.

    Example:
        inspect_row("raw.employees", "emp_id", "101")
        → {"row": {"emp_id": 101, "name": "Alice", "dept": "Sales"}}
    """
    try:
        # Convert value to appropriate type
        try:
            typed_value = int(key_value)
        except ValueError:
            typed_value = key_value

        return get_engine().inspect_row(table, key_column, typed_value)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def run_query(sql: str) -> dict:
    """
    ⚡ Run a custom SQL query (read-only).

    Use this for ad-hoc analysis not covered by other tools.

    Args:
        sql: SQL query to execute (SELECT only)

    Returns:
        Query results as list of rows.

    Example:
        run_query("SELECT * FROM raw.employees LIMIT 5")
        → {"rows": [...], "row_count": 5}

    SECURITY: Only SELECT queries allowed.
    """
    # Security: only allow SELECT queries
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith('SELECT'):
        return {"error": "Only SELECT queries are allowed"}

    # Block dangerous keywords
    dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
    for keyword in dangerous:
        if keyword in sql_upper:
            return {"error": f"Keyword '{keyword}' is not allowed"}

    try:
        df = get_engine().connector.execute(sql)
        return {
            "rows": df.to_dict(orient='records'),
            "row_count": len(df),
            "columns": list(df.columns)
        }
    except Exception as e:
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA QUALITY & BUSINESS LOGIC VALIDATION TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def detect_duplicates(table: str, columns: Optional[str] = None) -> dict:
    """
    🔍 Detect duplicate rows in a table.

    Automatically finds duplicate records and explains why they might exist.

    Args:
        table: Table to check (e.g., "silver.fact_jobs")
        columns: Optional comma-separated list of columns to check.
                If not provided, checks all columns for exact duplicates.
                Example: "emp_id,company_id"

    Returns:
        Duplicate detection results with explanation and recommendations.

    Example:
        detect_duplicates("silver.fact_jobs")
        → Shows all duplicate rows and explains why they exist

        detect_duplicates("silver.fact_jobs", "emp_id,company_id")
        → Checks for duplicates based on employee and company combination
    """
    try:
        validate_identifier(table, 'table')
        
        columns_list = None
        if columns:
            # Parse comma-separated columns
            columns_list = [col.strip() for col in columns.split(',')]
            for col in columns_list:
                validate_identifier(col.strip(), 'column')
        
        return get_engine().detect_duplicates(table, columns_list)
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def validate_business_rules(table: str, rules: Optional[str] = None) -> dict:
    """
    ✅ Validate data against business rules.

    Checks if data follows expected business logic (e.g., dates are valid, 
    numbers are positive, required fields are not null).

    Args:
        table: Table to validate (e.g., "silver.fact_jobs")
        rules: Optional comma-separated list of SQL WHERE clause conditions.
              If not provided, auto-detects common rules based on column names.
              Example: "salary > 0", "end_date >= start_date"

    Returns:
        Validation results for each rule with violation counts and examples.

    Example:
        validate_business_rules("silver.fact_jobs")
        → Auto-detects and validates common rules (positive salaries, valid dates, etc.)

        validate_business_rules("silver.fact_jobs", "salary > 0, end_date >= start_date")
        → Validates specific rules you provide
    """
    try:
        validate_identifier(table, 'table')
        
        rules_list = None
        if rules:
            # Parse comma-separated rules (more complex - handle with care)
            rules_list = [rule.strip() for rule in rules.split(',')]
        
        return get_engine().validate_business_rules(table, rules_list)
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def analyze_data_quality(table: str) -> dict:
    """
    📊 Comprehensive data quality analysis.

    Analyzes a table for common data quality issues:
    - Null values and their percentages
    - Outliers in numeric columns
    - Data type issues (e.g., dates stored as strings)
    - Duplicate detection
    - Overall quality score

    Args:
        table: Table to analyze (e.g., "conformed.company_stats")

    Returns:
        Comprehensive data quality report with metrics and recommendations.

    Example:
        analyze_data_quality("silver.fact_jobs")
        → {
            "row_count": 8,
            "null_analysis": {...},
            "numeric_statistics": {...},
            "quality_score": {"score": 85, "level": "GOOD"},
            "recommendations": [...]
          }
    """
    try:
        validate_identifier(table, 'table')
        return get_engine().analyze_data_quality(table)
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IMPACT ANALYSIS TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def analyze_impact(table_name: str) -> str:
    """
    💥 Analyze the blast radius of changing a table.

    Use this when the user wants to understand what will break or be affected
    if they modify a specific table — BEFORE they make the change.

    Traces all downstream tables that depend on the given table (recursively),
    calculates how many hops away each one is, and returns a risk assessment.

    Use this when the user says things like:
    - "I want to change table_a, what will it affect?"
    - "What's the impact of modifying silver.fact_jobs?"
    - "If I update this table, what breaks downstream?"
    - "Show me the blast radius of changing X"

    Args:
        table_name: The table you are planning to change (e.g., "silver.fact_jobs")

    Returns:
        A plain-English impact report showing all downstream tables,
        their hop distance, and an overall risk level.

    Example:
        analyze_impact("silver.fact_jobs")
        -> IMPACT ANALYSIS: silver.fact_jobs
           Risk: YELLOW — 3 downstream tables affected
           ...
    """
    try:
        engine = get_engine()
        visited: dict = {}  # table -> hop distance
        queue = [(table_name, 0)]

        while queue:
            current, distance = queue.pop(0)
            if current in visited:
                continue
            visited[current] = distance
            try:
                children = engine.get_downstream_tables(current)
                for child in children:
                    if child not in visited:
                        queue.append((child, distance + 1))
            except Exception:
                pass

        # Remove the source table itself
        visited.pop(table_name, None)

        if not visited:
            return (
                f"IMPACT ANALYSIS: {table_name}\n\n"
                f"Risk: GREEN — No downstream tables depend on this table.\n"
                f"Safe to change with no downstream impact."
            )

        total = len(visited)
        if total >= 5:
            risk = "RED"
            risk_note = f"{total} downstream tables affected — high blast radius, review carefully."
        elif total >= 2:
            risk = "YELLOW"
            risk_note = f"{total} downstream tables affected — proceed with caution."
        else:
            risk = "YELLOW"
            risk_note = f"{total} downstream table affected — minor impact."

        lines = [
            f"IMPACT ANALYSIS: {table_name}",
            f"",
            f"Risk: {risk} — {risk_note}",
            f"",
            f"Downstream tables that will be affected:",
        ]

        for table, hops in sorted(visited.items(), key=lambda x: x[1]):
            hop_label = f"hop {hops}" if hops > 1 else "direct dependency"
            lines.append(f"  - {table}  ({hop_label})")

        lines.append("")
        lines.append(f"Total impacted: {total} table(s)")

        return "\n".join(lines)

    except Exception as e:
        return f"Impact analysis error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PEER REVIEW TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def peer_review_setup() -> str:
    """
    🔧 Build peer review business context (run once when installing the MCP).

    Scans the project for ETL scripts (SQL, jobs, notebooks from config), builds
    lineage, and saves a context file so that when the user runs peer review,
    the agent can respond like a senior data engineer who knows the business.

    Run this once after adding the MCP to your AI tool, or after adding new ETL.

    Returns:
        Success message with path to the saved context file.
    """
    try:
        from scripts.peer_review.context_builder import build_peer_review_context
        path = build_peer_review_context(repo_path=PROJECT_ROOT, run_build=True)
        return (
            f"Peer review context built successfully.\n"
            f"Context saved to: {path}\n"
            f"When the user saves changes and runs peer review, the agent will use this "
            f"context and respond like a senior data engineer who knows the project."
        )
    except Exception as e:
        return f"Peer review setup error: {e}"


@mcp.tool()
def peer_review(staged_only: bool = False) -> str:
    """
    🎓 Run Senior Peer Review on your SQL code changes.

    Analyzes git changes and returns a full peer review report including:
    - Senior data engineer notes (when context was built via peer_review_setup)
    - What changed from → to, and which tables are impacted
    - Syntax errors (typos, unknown schema, unmatched parentheses)
    - Business / grain mismatch when grouping doesn't match downstream

    Use this when the user asks to:
    - "peer review my changes"
    - "check my code before committing"
    - "what's the impact of my changes?"
    - "review my SQL"

    Args:
        staged_only: If True, only review git-staged files.
                     If False (default), review ALL modified files.

    Returns:
        Formatted peer review report (Senior DE notes, changes, impact, incorrect, business).

    Example:
        peer_review()
        → Full report with Senior DE thoughts and change impact
    """
    try:
        from scripts.peer_review.peer_review import PeerReviewOrchestrator

        orchestrator = PeerReviewOrchestrator()
        advisory = orchestrator.review_changes(staged_only=staged_only)
        return advisory.formatted_output
    except Exception as e:
        return f"❌ Peer review error: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN ENTRY POINT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    """
    Start the MCP server and print a short config summary.

    NOTE:
        We intentionally **do not** import a hard-coded `DB_PATH` here.
        All connection details are managed by `config/db_config.py`
        and the `DebugEngine`, which read from `config.yml`.
    """
    from config.db_config import get_db_type, get_duckdb_config, get_databricks_config

    db_type = get_db_type()

    # Build a human-friendly database description
    if db_type == "duckdb":
        duck_conf = get_duckdb_config()
        db_description = duck_conf.get("path", "(no path configured)")
    elif db_type == "databricks":
        dbc_conf = get_databricks_config()
        host = dbc_conf.get("host", "unknown-host")
        catalog = dbc_conf.get("catalog", "hive_metastore")
        db_description = f"Databricks @ {host} (catalog={catalog})"
    else:
        db_description = f"(custom db_type={db_type})"

    print()
    print("=" * 60)
    print("🌐 DEBUG AI - MCP Server (Generic)")
    print("=" * 60)
    print()
    print(f"🗄️  DB Type: {db_type}")
    print(f"📁 Database / Connection: {db_description}")
    print()
    print("🔧 Available Tools:")
    print("   Discovery:")
    print("     • list_schemas       - Show available schemas")
    print("     • list_tables        - Show available tables")
    print("     • describe_table     - Show column info")
    print("     • get_row_count      - Count rows in a table")
    print()
    print("   Lineage:")
    print("     • explain_column     - How is a column calculated?")
    print("     • get_table_sources  - What feeds into this table?")
    print("     • get_lineage_tree   - Full dependency tree")
    print()
    print("   Debugging:")
    print("     • check_table_health - Debug data quality")
    print("     • inspect_row        - Look at specific data")
    print("     • run_query          - Custom SQL (read-only)")
    print()
    print("   Data Quality & Validation:")
    print("     • detect_duplicates  - Find duplicate rows")
    print("     • validate_business_rules - Check business logic")
    print("     • analyze_data_quality - Comprehensive quality analysis")
    print()
    print("   Impact Analysis:")
    print("     • analyze_impact     - Blast radius of changing a table (chat-friendly)")
    print()
    print("   Peer Review:")
    print("     • peer_review_setup  - Build business context (run once when installing MCP)")
    print("     • peer_review        - Review SQL changes for errors & impact")
    print()
    print("🚀 Starting server...")
    print("=" * 60)
    print()

    mcp.run()