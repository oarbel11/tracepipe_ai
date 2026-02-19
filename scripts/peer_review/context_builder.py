"""
Build business context for peer review: scan project, recognize ETL vs SQL, build business logic.

When the user runs peer review setup:
  - Review ALL files on the project (SQL, jobs, notebooks).
  - Recognize which are ETL scripts (CREATE TABLE/VIEW or in config ETL dirs) vs other SQL.
  - Build business logic (tables, grains, lineage).

When the user then saves a change and runs peer review, the agent (senior data engineer)
uses this context to give thoughts like a human who knows the business and catches typos.

USAGE:
    from scripts.peer_review.context_builder import build_peer_review_context
    path = build_peer_review_context(repo_path=".", run_build=True)
"""

import re
import sys
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config.db_config import get_all_etl_dirs
except Exception:
    get_all_etl_dirs = None

from scripts.peer_review.semantic_delta import SQLPatternMatcher


def _extract_group_by_columns(block: str) -> List[str]:
    """Extract GROUP BY column list (normalized)."""
    if not block or "GROUP BY" not in block.upper():
        return []
    m = re.search(
        r"\bGROUP\s+BY\s+([a-zA-Z0-9_,.\s]+?)(?=\s*HAVING|\s*ORDER|\s*LIMIT|;|\Z)",
        block, re.IGNORECASE | re.DOTALL
    )
    if not m:
        return []
    cols = []
    for part in re.split(r"\s*,\s*", m.group(1).strip()):
        part = part.strip()
        if not part:
            continue
        tokens = re.split(r"\s*\.\s*", part)
        cols.append(tokens[-1].lower() if tokens else part.lower())
    return cols


def _infer_grain(block: str, table_name: str) -> Optional[str]:
    """Infer grain from GROUP BY and table name."""
    cols = _extract_group_by_columns(block)
    col_str = " ".join(cols)
    table_lower = table_name.lower()
    if any(x in cols or x in col_str for x in ("company_id", "company_name")):
        return "company"
    if any(x in cols or x in col_str for x in ("emp_id", "full_name")):
        return "employee"
    if any(x in cols or x in col_str for x in ("dept_id", "dept_name")):
        return "department"
    if "industry" in cols or "industry" in col_str:
        return "industry"
    if "job_id" in cols or "job_id" in col_str:
        return "job"
    if "company" in table_lower and ("stats" in table_lower or "dim" in table_lower):
        return "company"
    if "employee" in table_lower or "career" in table_lower or "dim_employees" in table_lower:
        return "employee"
    if "department" in table_lower or "dept" in table_lower:
        return "department"
    if "industry" in table_lower:
        return "industry"
    if "fact_jobs" in table_lower or "job" in table_lower:
        return "job"
    return None


def _split_sql_into_blocks(sql: str) -> Dict[str, str]:
    """Split SQL into per-table blocks."""
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


def _is_etl_sql(content: str) -> bool:
    """True if SQL content defines pipeline objects (CREATE TABLE/VIEW)."""
    if not content or not content.strip():
        return False
    u = content.upper()
    return "CREATE TABLE" in u or "CREATE VIEW" in u or "CREATE OR REPLACE TABLE" in u or "CREATE OR REPLACE VIEW" in u


def discover_etl_files(repo_path: str) -> List[Dict[str, str]]:
    """Discover ETL scripts from config (sql_dir, jobs_dir, notebooks_dir)."""
    entries = []
    if not get_all_etl_dirs:
        return entries
    base = Path(repo_path)
    dirs = get_all_etl_dirs()
    for kind, dir_path in dirs.items():
        if not dir_path or not dir_path.exists():
            continue
        if kind == "sql_dir":
            for f in dir_path.rglob("*.sql"):
                if f.is_file():
                    try:
                        r = f.relative_to(base)
                    except ValueError:
                        r = f
                    entries.append({"path": str(r).replace("\\", "/"), "type": "etl_sql"})
        elif kind == "jobs_dir":
            for f in dir_path.rglob("*.py"):
                if f.is_file():
                    try:
                        r = f.relative_to(base)
                    except ValueError:
                        r = f
                    entries.append({"path": str(r).replace("\\", "/"), "type": "job"})
        elif kind == "notebooks_dir":
            for f in dir_path.rglob("*"):
                if f.is_file() and f.suffix in (".ipynb", ".py"):
                    try:
                        r = f.relative_to(base)
                    except ValueError:
                        r = f
                    entries.append({"path": str(r).replace("\\", "/"), "type": "notebook"})
    return entries


def scan_project_sql(repo_path: str) -> List[Dict[str, str]]:
    """
    Scan the whole project for SQL files and classify as ETL script vs other SQL.
    ETL = in config sql_dir or content has CREATE TABLE/VIEW.
    """
    base = Path(repo_path)
    config_etl_paths: set = set()
    if get_all_etl_dirs:
        dirs = get_all_etl_dirs()
        sql_dir = dirs.get("sql_dir")
        if sql_dir and sql_dir.exists():
            for f in sql_dir.rglob("*.sql"):
                if f.is_file():
                    try:
                        r = f.relative_to(base)
                        config_etl_paths.add(str(r).replace("\\", "/"))
                    except ValueError:
                        config_etl_paths.add(str(f))
    skip_dirs = {".git", "__pycache__", ".venv", "venv", "node_modules", ".tox"}
    all_sql: List[Dict[str, str]] = []
    for sql_file in base.rglob("*.sql"):
        if not sql_file.is_file():
            continue
        if any(p in sql_file.parts for p in skip_dirs):
            continue
        try:
            rel = sql_file.relative_to(base)
            path_str = str(rel).replace("\\", "/")
        except ValueError:
            path_str = str(sql_file)
        if path_str in config_etl_paths:
            all_sql.append({"path": path_str, "type": "etl_sql"})
            continue
        try:
            content = sql_file.read_text(encoding="utf-8")
        except Exception:
            all_sql.append({"path": path_str, "type": "sql"})
            continue
        all_sql.append({"path": path_str, "type": "etl_sql" if _is_etl_sql(content) else "sql"})
    return all_sql


def build_tables_business_logic(repo_path: str, etl_files: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    """From ETL SQL files, build table -> { file, grain, description }. Only ETL SQL (etl_sql or sql)."""
    tables: Dict[str, Dict[str, Any]] = {}
    base = Path(repo_path)
    for entry in etl_files:
        if entry.get("type") not in ("etl_sql", "sql"):
            continue
        path = base / entry["path"]
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            continue
        blocks = _split_sql_into_blocks(content)
        for table_name, block in blocks.items():
            grain = _infer_grain(block, table_name)
            # Optional: first line of comment before CREATE as description
            desc = None
            if block.strip().startswith("--"):
                first_line = block.strip().split("\n")[0].strip()
                if first_line.startswith("--"):
                    desc = first_line.lstrip("- ").strip()[:120]
            tables[table_name] = {
                "file": entry["path"],
                "grain": grain,
                "description": desc,
            }
    return tables


def get_lineage_summary(debug_engine: Any) -> Dict[str, List[str]]:
    """Build table -> list of downstream table names from lineage DB."""
    summary: Dict[str, List[str]] = {}
    if not debug_engine:
        return summary
    try:
        tables = debug_engine.list_tables()
        for t in tables or []:
            schema = t.get("table_schema", "") or ""
            name = t.get("table_name", "") or ""
            full = f"{schema}.{name}" if schema else name
            try:
                down = debug_engine.get_downstream_tables(full)
                if down:
                    summary[full] = list(down)
            except Exception:
                pass
    except Exception:
        pass
    return summary


def build_peer_review_context(
    repo_path: Optional[str] = None,
    output_path: Optional[str] = None,
    run_build: bool = True,
) -> str:
    """
    Discover ETL files, build table business logic, optionally run lineage build, save context.

    Returns:
        Path to the saved context JSON file.
    """
    repo_path = repo_path or str(PROJECT_ROOT)
    if output_path is None:
        config_dir = Path(repo_path) / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(config_dir / "peer_review_context.json")

    # 1. Review all files on the project: recognize ETL scripts vs other SQL
    config_etl = discover_etl_files(repo_path)
    config_paths = {e["path"] for e in config_etl}
    project_sql = scan_project_sql(repo_path)
    etl_files = list(config_etl)
    for e in project_sql:
        if e["type"] == "etl_sql" and e["path"] not in config_paths:
            etl_files.append(e)
    other_sql_files = [e for e in project_sql if e["type"] == "sql"]

    # 2. Build business logic from ETL scripts only
    sql_for_tables = [e for e in etl_files if e.get("type") in ("etl_sql", "sql")]
    tables = build_tables_business_logic(repo_path, sql_for_tables)

    # 3. Optionally run build to refresh lineage, then get lineage summary
    lineage_summary: Dict[str, List[str]] = {}
    if run_build:
        try:
            from scripts.build_metadata import MetadataBuilder
            from config.db_config import get_duckdb_config, get_db_type, get_sql_dir
            if get_db_type() == "duckdb":
                db_path = get_duckdb_config().get("path", "")
                sql_dir = get_sql_dir()
                if db_path and sql_dir and sql_dir.exists():
                    builder = MetadataBuilder(db_path=db_path, sql_dir=str(sql_dir))
                    builder.build()
        except Exception:
            pass
    try:
        from scripts.debug_engine import DebugEngine
        engine = DebugEngine()
        lineage_summary = get_lineage_summary(engine)
    except Exception:
        pass

    context = {
        "etl_files": etl_files,
        "other_sql_files": other_sql_files,
        "tables": tables,
        "lineage_summary": lineage_summary,
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(context, f, indent=2)
    return output_path
