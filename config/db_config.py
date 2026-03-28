import os
import yaml
from pathlib import Path


def load_config() -> dict:
    """Load configuration from config.yml."""
    config_path = Path(__file__).parent / "config.yml"
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return {}


def get_databricks_config() -> dict:
    """Get Databricks connection parameters from config or environment."""
    config = load_config()
    databricks_config = config.get('databricks', {})
    
    return {
        'server_hostname': (
            databricks_config.get('server_hostname') or 
            os.getenv('DATABRICKS_SERVER_HOSTNAME')
        ),
        'http_path': (
            databricks_config.get('http_path') or 
            os.getenv('DATABRICKS_HTTP_PATH')
        ),
        'access_token': (
            databricks_config.get('access_token') or 
            os.getenv('DATABRICKS_TOKEN')
        )
    }


def get_duckdb_path() -> str:
    """Get DuckDB database path."""
    config = load_config()
    return config.get('duckdb', {}).get('path', 'tracepipe.duckdb')
