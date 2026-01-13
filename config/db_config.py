"""
Database Configuration
======================
Reads connection settings from config.yml
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

import yaml


# Find config file and project root
CONFIG_DIR = Path(__file__).parent
PROJECT_ROOT = CONFIG_DIR.parent
CONFIG_FILE = CONFIG_DIR / 'config.yml'

# Add project root to path for imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def load_config() -> Dict[str, Any]:
    """Load configuration from config.yml"""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")
    
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)


def get_db_type() -> str:
    """Get the configured database type."""
    config = load_config()
    return config.get('db_type', 'duckdb')


def get_duckdb_config() -> Dict[str, str]:
    """Get DuckDB configuration."""
    config = load_config()
    duckdb_config = config.get('duckdb', {})
    
    # Resolve relative path
    path = duckdb_config.get('path', '')
    if path and not Path(path).is_absolute():
        path = str(CONFIG_DIR.parent / path)
    
    return {'path': path}


def get_databricks_config() -> Dict[str, str]:
    """Get Databricks configuration."""
    config = load_config()
    return config.get('databricks', {})


# ============================================
# Path Resolution Helper
# ============================================

def _resolve_path(path_str: str) -> Optional[Path]:
    """Helper to resolve a path from config (handles relative paths)."""
    if not path_str:
        return None
    
    path = Path(path_str)
    if not path.is_absolute():
        path = CONFIG_DIR.parent / path_str
    
    return path


# ============================================
# ETL Jobs Configuration
# ============================================

def get_sql_dir() -> Optional[Path]:
    """Get the SQL files directory path."""
    config = load_config()
    return _resolve_path(config.get('sql_dir', ''))


def get_jobs_dir() -> Optional[Path]:
    """Get the Spark/PySpark jobs directory path."""
    config = load_config()
    return _resolve_path(config.get('jobs_dir', ''))


def get_notebooks_dir() -> Optional[Path]:
    """Get the exported notebooks directory path."""
    config = load_config()
    return _resolve_path(config.get('notebooks_dir', ''))


def get_databricks_jobs() -> List[Dict[str, str]]:
    """
    Get the list of Databricks workspace job paths.
    
    Returns:
        List of dicts with 'workspace_path' and 'description' keys
    """
    config = load_config()
    jobs = config.get('databricks_jobs', [])
    return jobs if jobs else []


def get_all_etl_dirs() -> Dict[str, Optional[Path]]:
    """
    Get all configured ETL directories.
    
    Returns:
        Dict with keys: 'sql_dir', 'jobs_dir', 'notebooks_dir'
    """
    return {
        'sql_dir': get_sql_dir(),
        'jobs_dir': get_jobs_dir(),
        'notebooks_dir': get_notebooks_dir()
    }


def get_etl_dir() -> Optional[Path]:
    """
    Get the primary ETL directory (backward compatibility).
    Checks sql_dir first, then jobs_dir, then notebooks_dir.
    """
    # Try sql_dir first
    sql_dir = get_sql_dir()
    if sql_dir and sql_dir.exists():
        return sql_dir
    
    # Then jobs_dir
    jobs_dir = get_jobs_dir()
    if jobs_dir and jobs_dir.exists():
        return jobs_dir
    
    # Then notebooks_dir
    notebooks_dir = get_notebooks_dir()
    if notebooks_dir and notebooks_dir.exists():
        return notebooks_dir
    
    return None


# ============================================
# Lineage Source Configuration
# ============================================

def get_lineage_source() -> str:
    """
    Get the configured lineage source.
    
    Returns:
        'local' - Parse SQL/Spark files from local folders
        'databricks' - Use Unity Catalog system tables
        'auto' - Auto-detect based on local file existence
    """
    config = load_config()
    source = config.get('lineage_source', 'auto')
    
    if source == 'auto':
        # Check if any local directories have files
        sql_dir = get_sql_dir()
        jobs_dir = get_jobs_dir()
        notebooks_dir = get_notebooks_dir()
        
        has_sql = sql_dir and sql_dir.exists() and any(sql_dir.glob('*.sql'))
        has_jobs = jobs_dir and jobs_dir.exists() and any(jobs_dir.glob('*.py'))
        has_notebooks = notebooks_dir and notebooks_dir.exists()
        
        if has_sql or has_jobs or has_notebooks:
            return 'local'
        elif get_db_type() == 'databricks':
            return 'databricks'
        else:
            return 'local'
    
    return source


# ============================================
# Database Connection
# ============================================

def get_connection() -> Any:
    """
    Get database connection based on config.
    
    Returns:
        Database connection (DuckDB or Databricks)
    """
    db_type = get_db_type()
    
    if db_type == 'duckdb':
        import duckdb
        config = get_duckdb_config()
        return duckdb.connect(config['path'], read_only=True)
    
    elif db_type == 'databricks':
        from databricks import sql
        config = get_databricks_config()
        
        return sql.connect(
            server_hostname=config['host'],
            http_path=config['http_path'],
            access_token=config['token'],
            catalog=config.get('catalog', 'hive_metastore')
        )
    
    else:
        raise ValueError(f"Unknown database type: {db_type}")


# For convenience
DB_TYPE = get_db_type()


if __name__ == '__main__':
    print("Database Configuration")
    print("=" * 50)
    
    config = load_config()
    db_type = config.get('db_type', 'duckdb')
    
    print(f"\n📊 Database Type: {db_type}")
    print(f"🔗 Lineage Source: {get_lineage_source()}")
    
    # Show ETL directories
    print("\n📁 ETL Directories:")
    etl_dirs = get_all_etl_dirs()
    for name, path in etl_dirs.items():
        if path:
            status = "✅ exists" if path.exists() else "❌ not found"
            print(f"   {name}: {path} ({status})")
        else:
            print(f"   {name}: (not configured)")
    
    # Show Databricks jobs paths
    db_jobs = get_databricks_jobs()
    if db_jobs:
        print("\n☁️ Databricks Jobs:")
        for job in db_jobs:
            print(f"   {job.get('workspace_path', 'N/A')}: {job.get('description', '')}")
    
    # Database specific config
    if db_type == 'duckdb':
        duckdb_config = get_duckdb_config()
        print(f"\n🦆 DuckDB Path: {duckdb_config['path']}")
        
    elif db_type == 'databricks':
        databricks_config = get_databricks_config()
        print(f"\n☁️ Databricks:")
        print(f"   Host: {databricks_config.get('host')}")
        print(f"   Catalog: {databricks_config.get('catalog')}")
        print(f"   HTTP Path: {databricks_config.get('http_path')}")
        print(f"   Token: ***{databricks_config.get('token', '')[-4:]}")
    
    print("\n✅ Configuration loaded successfully")
    
    # Test connection
    print("\n🔌 Testing connection...")
    from scripts.debug_engine import DebugEngine
    engine = DebugEngine()
    schemas = engine.list_schemas()
    print(f"✅ Schemas found: {schemas}")