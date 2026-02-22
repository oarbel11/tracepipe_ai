#!/usr/bin/env python3
"""
Tracepipe AI - Interactive Setup Wizard
Runs after installation to configure database and ETL paths.
"""

import os
import sys
import json
from pathlib import Path

try:
    import yaml
except ImportError:
    print("❌ PyYAML is required. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyyaml"])
    import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")

def print_success(text):
    """Print success message."""
    print(f"✅ {text}")

def print_info(text):
    """Print info message."""
    print(f"ℹ️  {text}")

def get_input(prompt, default=None, required=True):
    """Get user input with optional default."""
    if default:
        full_prompt = f"{prompt} [{default}]: "
    else:
        full_prompt = f"{prompt}: "
    
    while True:
        value = input(full_prompt).strip()
        if value:
            return value
        elif default:
            return default
        elif not required:
            return ""
        else:
            print("   This field is required. Please enter a value.")

def get_path_input(prompt, default=None, required=True, must_exist=False):
    """Get path input and validate."""
    while True:
        path = get_input(prompt, default, required)
        if not path and not required:
            return ""
        
        path_obj = Path(path).expanduser()
        
        if must_exist and not path_obj.exists():
            print(f"   ⚠️  Path does not exist: {path}")
            retry = input("   Continue anyway? (y/N): ").strip().lower()
            if retry != 'y':
                continue
        
        return str(path_obj.resolve())

def configure_duckdb():
    """Configure DuckDB settings."""
    print_header("DuckDB Configuration")
    
    db_path = get_path_input(
        "Enter path to your DuckDB database file",
        default="companies_data_duckdb/corporate.duckdb",
        required=True,
        must_exist=False
    )
    
    return {
        'db_type': 'duckdb',
        'lineage_source': 'local',
        'duckdb': {
            'path': db_path
        },
        'databricks': {
            'host': '',
            'token': '',
            'http_path': '',
            'catalog': ''
        }
    }

def configure_databricks():
    """Configure Databricks settings."""
    print_header("Databricks Configuration")
    
    print("You'll need the following from your Databricks workspace:")
    print("  1. Host (workspace URL without https://)")
    print("  2. Token (Settings → Developer → Access Tokens)")
    print("  3. HTTP Path (SQL Warehouses → Connection Details)")
    print("  4. Catalog (Unity Catalog name)\n")
    
    host = get_input("Databricks host (e.g., your-workspace.cloud.databricks.com)")
    token = get_input("Access token (dapi...)", required=True)
    http_path = get_input("HTTP Path (e.g., /sql/1.0/warehouses/xxx)")
    catalog = get_input("Unity Catalog name")
    
    return {
        'db_type': 'databricks',
        'lineage_source': 'databricks',
        'duckdb': {
            'path': ''
        },
        'databricks': {
            'host': host,
            'token': token,
            'http_path': http_path,
            'catalog': catalog
        }
    }

def configure_etl_paths(config):
    """Configure ETL file paths."""
    print_header("ETL Files Configuration")
    
    print("Where are your SQL/ETL files located?")
    print("  This is where Tracepipe AI will look for CREATE TABLE statements")
    print("  to build lineage metadata.\n")
    
    if config['db_type'] == 'duckdb':
        default_etl = "companies_data_duckdb/etl"
    else:
        default_etl = ""
    
    sql_dir = get_path_input(
        "Enter path to your SQL/ETL files directory",
        default=default_etl,
        required=False,
        must_exist=False
    )
    
    config['sql_dir'] = sql_dir if sql_dir else ""
    config['jobs_dir'] = ""
    config['notebooks_dir'] = ""
    
    return config

def write_config(config):
    """Write configuration to config.yml."""
    config_path = PROJECT_ROOT / 'config' / 'config.yml'
    
    # Read existing config to preserve peer_review settings
    existing_config = {}
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                existing_config = yaml.safe_load(f) or {}
        except:
            pass
    
    # Merge with existing peer_review config if it exists
    if 'peer_review' in existing_config:
        config['peer_review'] = existing_config['peer_review']
    else:
        # Default peer_review config
        config['peer_review'] = {
            'enabled': True,
            'llm': {
                'provider': 'gemini',
                'model': 'gemini-1.5-flash',
                'api_key_env': 'GEMINI_API_KEY'
            },
            'risk_levels': {
                'block_commit_on': 'RED',
                'technical_severity_threshold': 'MEDIUM',
                'business_severity_threshold': 'MEDIUM'
            },
            'cache_lineage': True,
            'max_downstream_depth': 10,
            'exclude_patterns': ['*/test/*', '*/migrations/*', '*.backup.sql']
        }
    
    # Add databricks_jobs if not present
    if 'databricks_jobs' not in config:
        config['databricks_jobs'] = []
    
    # Write config
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_path, 'w') as f:
        f.write("# ============================================\n")
        f.write("# Tracepipe AI - Database Configuration\n")
        f.write("# ============================================\n")
        f.write("# Generated by setup wizard\n\n")
        
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print_success(f"Configuration saved to: {config_path}")
    return config_path

def detect_ai_tools():
    """Detect which AI tools are installed."""
    detected = []
    
    # Check for Cursor
    if sys.platform == 'win32':
        cursor_config = Path(os.environ.get('APPDATA', '')) / 'Cursor' / 'mcp.json'
    else:
        cursor_config = Path.home() / '.cursor' / 'mcp.json'
    
    if cursor_config.parent.exists():
        detected.append(('Cursor', cursor_config))
    
    # Check for Claude Desktop
    if sys.platform == 'win32':
        claude_config = Path(os.environ.get('APPDATA', '')) / 'Claude' / 'claude_desktop_config.json'
    elif sys.platform == 'darwin':
        claude_config = Path.home() / 'Library' / 'Application Support' / 'Claude' / 'claude_desktop_config.json'
    else:
        claude_config = Path.home() / '.config' / 'claude' / 'claude_desktop_config.json'
    
    if claude_config.parent.exists():
        detected.append(('Claude Desktop', claude_config))
    
    return detected

def configure_mcp_auto(mcp_path):
    """Automatically configure MCP for detected AI tools."""
    detected_tools = detect_ai_tools()
    
    if not detected_tools:
        print_header("MCP Server Setup")
        print("No AI tools detected (Cursor or Claude Desktop).")
        print("You can configure MCP manually later.\n")
        show_mcp_manual_instructions(mcp_path)
        return False
    
    print_header("MCP Server Setup")
    print("I found the following AI tools installed:\n")
    for i, (name, config_path) in enumerate(detected_tools, 1):
        exists = "✅ (config file exists)" if config_path.exists() else "⚠️  (will create)"
        print(f"  {i}. {name} - {config_path} {exists}")
    
    print("\nWould you like me to automatically configure Tracepipe AI for these tools?")
    response = input("   Auto-configure MCP? (Y/n): ").strip().lower()
    
    if response and response != 'y':
        show_mcp_manual_instructions(mcp_path)
        return False
    
    configured = []
    mcp_config = {
        "mcpServers": {
            "debug-ai": {
                "command": "python",
                "args": [str(mcp_path)]
            }
        }
    }
    
    for tool_name, config_path in detected_tools:
        try:
            # Read existing config if it exists
            existing_config = {}
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        existing_config = json.load(f)
                except:
                    pass
            
            # Merge with existing mcpServers
            if 'mcpServers' not in existing_config:
                existing_config['mcpServers'] = {}
            
            existing_config['mcpServers']['debug-ai'] = mcp_config['mcpServers']['debug-ai']
            
            # Write config
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(existing_config, f, indent=2)
            
            configured.append(tool_name)
            print_success(f"Configured {tool_name} at: {config_path}")
        except Exception as e:
            print(f"   ⚠️  Failed to configure {tool_name}: {e}")
    
    if configured:
        print(f"\n✅ Successfully configured MCP for: {', '.join(configured)}")
        print("\n📋 Next steps:")
        print("   1. Restart your AI tool(s)")
        print("   2. Then you can ask questions like:")
        print("      - 'How is risk_level calculated?'")
        print("      - 'Run peer review'")
        return True
    
    return False

def show_mcp_manual_instructions(mcp_path):
    """Show manual MCP setup instructions."""
    print_header("MCP Server Setup (Manual)")
    
    print("To use Tracepipe AI with AI tools (Cursor, Claude Desktop, etc.):\n")
    print("1. Add this to your AI tool's MCP configuration file:\n")
    
    if sys.platform == 'win32':
        print("   For Cursor: %APPDATA%\\Cursor\\mcp.json")
        print("   For Claude Desktop: %APPDATA%\\Claude\\claude_desktop_config.json")
    elif sys.platform == 'darwin':
        print("   For Cursor: ~/.cursor/mcp.json")
        print("   For Claude Desktop: ~/Library/Application Support/Claude/claude_desktop_config.json")
    else:
        print("   For Cursor: ~/.cursor/mcp.json")
        print("   For Claude Desktop: ~/.config/claude/claude_desktop_config.json")
    
    print("\n   Configuration:\n")
    print("   {")
    print('     "mcpServers": {')
    print('       "debug-ai": {')
    print('         "command": "python",')
    print(f'         "args": ["{mcp_path}"]')
    print("       }")
    print("     }")
    print("   }\n")
    print("2. Restart your AI tool\n")
    print("3. Then you can ask questions like:")
    print("   - 'How is risk_level calculated?'")
    print("   - 'Run peer review'")

def run_lineage_build(config):
    """Auto-run lineage metadata build after configuration."""
    print_header("Building Lineage Metadata")
    
    try:
        # Import config loader to get effective config (reads config.yml)
        from config.db_config import load_config as load_db_config
        db_config = load_db_config()
        
        db_path = db_config.get('duckdb', {}).get('path', '') if config['db_type'] == 'duckdb' else ''
        sql_dir = db_config.get('sql_dir', config.get('sql_dir', ''))
        
        if not sql_dir:
            print("   ⚠️  No SQL directory configured. Skipping lineage build.")
            print("   You can run it later: python scripts/cli.py build\n")
            return False
        
        print(f"   Scanning SQL files in: {sql_dir}")
        print(f"   Database: {db_path}\n")
        
        from scripts.build_metadata import MetadataBuilder
        
        builder = MetadataBuilder(
            db_path=db_path,
            sql_dir=sql_dir,
            meta_schema='meta'
        )
        builder.build()
        
        print("\n   ✅ Lineage metadata built successfully!")
        return True
        
    except Exception as e:
        print(f"\n   ⚠️  Lineage build encountered an issue: {e}")
        print("   You can retry later: python scripts/cli.py build")
        return False


def run_peer_review_setup(config):
    """Prompt user for optional peer review setup."""
    print_header("Peer Review Setup")
    
    print("Peer Review gives you senior-level code reviews before you commit.")
    print("It catches SQL errors, downstream impact, and business logic issues.\n")
    
    response = input("   Set up peer review now? (Y/n): ").strip().lower()
    
    if response and response != 'y':
        print("\n   Skipped. You can set it up anytime by running:")
        print("   python scripts/cli.py peer-review setup\n")
        return
    
    try:
        from scripts.peer_review.context_builder import build_peer_review_context
        repo_path = str(PROJECT_ROOT)
        path = build_peer_review_context(repo_path=repo_path, run_build=True)
        print(f"\n   ✅ Peer review context saved to: {path}")
        print("\n   When you run peer review (or ask your AI tool to review),")
        print("   it will respond like a senior data engineer who knows your project.")
    except Exception as e:
        print(f"\n   ⚠️  Peer review setup encountered an issue: {e}")
        print("   You can retry anytime: python scripts/cli.py peer-review setup")


def main():
    """Main setup wizard."""
    print("\n" + "=" * 70)
    print(" " * 18 + "Tracepipe AI Setup Wizard")
    print("=" * 70 + "\n")
    
    print("This wizard will help you configure Tracepipe AI for your database.")
    print("You can always edit config/config.yml later to change settings.\n")
    
    # Step 1: Choose database type
    print_header("Database Selection")
    print("Which database are you using?")
    print("  1. DuckDB (local file-based database)")
    print("  2. Databricks (cloud workspace)")
    
    while True:
        choice = input("\nEnter choice (1 or 2): ").strip()
        if choice == '1':
            config = configure_duckdb()
            break
        elif choice == '2':
            config = configure_databricks()
            break
        else:
            print("   Invalid choice. Please enter 1 or 2.")
    
    # Step 2: Configure ETL paths
    config = configure_etl_paths(config)
    
    # Step 3: Write configuration
    config_path = write_config(config)
    
    # Step 4: Configure MCP automatically or show manual instructions
    mcp_path = (PROJECT_ROOT / 'mcp_server.py').resolve()
    auto_configured = configure_mcp_auto(mcp_path)
    if not auto_configured:
        # If auto-config failed or user declined, show manual instructions
        show_mcp_manual_instructions(mcp_path)
    
    # Step 5: Auto-run lineage build
    run_lineage_build(config)
    
    # Step 6: Ask about peer review setup (optional)
    run_peer_review_setup(config)
    
    # Done
    print("\n-------------------------------------------------------------------")
    print("\n\u2705 Setup complete! You're ready to use Tracepipe AI.")
    print("\n   Tip: You can set up peer review anytime by running:")
    print("   python scripts/cli.py peer-review setup\n")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Setup cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
