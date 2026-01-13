"""
Database Configuration
======================
Reads connection settings from config.yml
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional

import yaml


# Find config file
CONFIG_DIR = Path(__file__).parent
CONFIG_FILE = CONFIG_DIR / 'config.yml'


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
    
    print(f"\nDatabase Type: {db_type}")
    
    if db_type == 'duckdb':
        duckdb_config = get_duckdb_config()
        print(f"Path: {duckdb_config['path']}")
        
    elif db_type == 'databricks':
        databricks_config = get_databricks_config()
        print(f"Host: {databricks_config.get('host')}")
        print(f"Catalog: {databricks_config.get('catalog')}")
        print(f"HTTP Path: {databricks_config.get('http_path')}")
        print(f"Token: ***{databricks_config.get('token', '')[-4:]}")
    
    print("\n✅ Configuration loaded successfully")