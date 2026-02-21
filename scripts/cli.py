"""
╔══════════════════════════════════════════════════════════════════╗
║                         cli.py                                    ║
║                 🎮 Tracepipe AI Command Line Interface            ║
╠══════════════════════════════════════════════════════════════════╣
║  Makes it easy for ANY client to set up and use Tracepipe AI!     ║
║                                                                   ║
║  COMMANDS:                                                        ║
║    init     - Initialize project with custom paths                ║
║    config   - Show/update configuration                           ║
║    scan     - Discover database structure                         ║
║    build    - Build metadata from SQL files                       ║
║    query    - Query lineage interactively                         ║
║    serve    - Start MCP server                                    ║
║    test     - Run all tests                                       ║
║    peer-review - Run senior peer review on code changes           ║
╚══════════════════════════════════════════════════════════════════╝

USAGE:
    python cli.py init --db ./data/warehouse.duckdb --sql ./etl/
    python cli.py scan
    python cli.py build
    python cli.py query "schema.table" "column"
    python cli.py serve
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Optional

# cli.py is now in scripts/ folder
# PROJECT_ROOT is the parent (tracepipe_ai/)
# SCRIPTS_DIR is where cli.py lives (tracepipe_ai/scripts/)
SCRIPTS_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPTS_DIR.parent.resolve()

# Add both to path for imports
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(PROJECT_ROOT))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG FILE MANAGEMENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Config file lives in project root (not scripts folder)
CONFIG_FILE = PROJECT_ROOT / '.tracepipe_ai_config.json'

DEFAULT_CONFIG = {
    'db_path': None,
    'db_type': 'duckdb',
    'sql_dir': None,
    'meta_schema': 'meta',
}


def load_config() -> dict:
    """Load configuration from file."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r') as f:
            saved = json.load(f)
            return {**DEFAULT_CONFIG, **saved}
    return DEFAULT_CONFIG.copy()


def save_config(config: dict):
    """Save configuration to file."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    print(f"✅ Configuration saved to {CONFIG_FILE}")


def get_effective_config() -> dict:
    """Get config with environment variable overrides."""
    config = load_config()

    # Environment variables take priority
    if os.getenv('TRACEPIPE_AI_DB_PATH') or os.getenv('DEBUG_AI_DB_PATH'):
        config['db_path'] = os.getenv('TRACEPIPE_AI_DB_PATH') or os.getenv('DEBUG_AI_DB_PATH')
    if os.getenv('TRACEPIPE_AI_ETL_DIR') or os.getenv('DEBUG_AI_ETL_DIR'):
        config['sql_dir'] = os.getenv('TRACEPIPE_AI_ETL_DIR') or os.getenv('DEBUG_AI_ETL_DIR')
    if os.getenv('TRACEPIPE_AI_DB_TYPE') or os.getenv('DEBUG_AI_DB_TYPE'):
        config['db_type'] = os.getenv('TRACEPIPE_AI_DB_TYPE') or os.getenv('DEBUG_AI_DB_TYPE')

    return config


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def cmd_init(args):
    """Initialize Tracepipe AI with custom paths."""
    print()
    print("=" * 60)
    print("🚀 DEBUG AI - Initialization")
    print("=" * 60)

    config = load_config()

    # Database path
    if args.db:
        db_path = Path(args.db).resolve()
        if db_path.exists():
            config['db_path'] = str(db_path)
            print(f"✅ Database: {db_path}")
        else:
            print(f"⚠️  Database not found: {db_path}")
            print("   Will be created when you run 'build'")
            config['db_path'] = str(db_path)

    # SQL directory
    if args.sql:
        sql_dir = Path(args.sql).resolve()
        if sql_dir.exists():
            config['sql_dir'] = str(sql_dir)
            sql_count = len(list(sql_dir.glob('*.sql')))
            print(f"✅ SQL Directory: {sql_dir} ({sql_count} files)")
        else:
            print(f"❌ SQL directory not found: {sql_dir}")

    # Database type
    if args.db_type:
        config['db_type'] = args.db_type
        print(f"✅ Database type: {args.db_type}")

    # Meta schema
    if args.meta_schema:
        config['meta_schema'] = args.meta_schema
        print(f"✅ Metadata schema: {args.meta_schema}")

    # Save config
    save_config(config)

    print()
    print("Next steps:")
    print("  1. python cli.py scan    # See what's in your database")
    print("  2. python cli.py build   # Build metadata from SQL files")
    print("  3. python cli.py query   # Query lineage")
    print()


def cmd_config(args):
    """Show or update configuration."""
    config = get_effective_config()

    if args.show:
        print()
        print("=" * 60)
        print("📒 DEBUG AI - Current Configuration")
        print("=" * 60)
        print()
        for key, value in config.items():
            status = "✅" if value else "❌"
            print(f"  {status} {key}: {value or 'NOT SET'}")
        print()
        print(f"📁 Config file: {CONFIG_FILE}")
        print()
        return

    # Update specific values
    updated = False

    if args.db:
        config['db_path'] = str(Path(args.db).resolve())
        updated = True

    if args.sql:
        config['sql_dir'] = str(Path(args.sql).resolve())
        updated = True

    if args.db_type:
        config['db_type'] = args.db_type
        updated = True

    if args.meta_schema:
        config['meta_schema'] = args.meta_schema
        updated = True

    if updated:
        save_config(config)
    else:
        print("No changes. Use --show to see current config.")


def cmd_scan(args):
    """Scan database and show structure."""
    config = get_effective_config()

    if not config['db_path']:
        print("❌ No database configured!")
        print("   Run: python cli.py init --db /path/to/database.duckdb")
        return

    print()
    print("=" * 60)
    print("🔍 DEBUG AI - Database Scan")
    print("=" * 60)
    print(f"\n📁 Database: {config['db_path']}")

    try:
        from debug_engine import DebugEngine
        engine = DebugEngine(db_path=config['db_path'])

        # List schemas
        print("\n📂 Schemas:")
        schemas = engine.list_schemas()
        for s in schemas:
            print(f"   • {s}")

        # List tables with row counts
        print("\n📋 Tables:")
        tables = engine.list_tables()
        for t in tables:
            full_name = f"{t['table_schema']}.{t['table_name']}"
            try:
                count = engine.get_row_count(full_name)
                print(f"   • {full_name} ({count:,} rows)")
            except:
                print(f"   • {full_name}")

        # Check metadata
        print("\n📊 Metadata Status:")
        meta_status = engine._check_metadata_exists()
        for name, exists in meta_status.items():
            status = "✅" if exists else "❌"
            print(f"   {status} {name}")

        print()

    except FileNotFoundError as e:
        print(f"\n❌ {e}")
    except Exception as e:
        print(f"\n❌ Error: {e}")


def cmd_build(args):
    """Build metadata from SQL files."""
    config = get_effective_config()

    if not config['db_path']:
        print("❌ No database configured!")
        print("   Run: python cli.py init --db /path/to/database.duckdb")
        return

    sql_dir = args.sql or config['sql_dir']

    if not sql_dir:
        print("❌ No SQL directory configured!")
        print("   Run: python cli.py init --sql /path/to/sql/")
        print("   Or:  python cli.py build --sql /path/to/sql/")
        return

    from scripts.build_metadata import MetadataBuilder

    builder = MetadataBuilder(
        db_path=config['db_path'],
        sql_dir=sql_dir,
        meta_schema=config['meta_schema']
    )

    builder.build()


def cmd_query(args):
    """Query column lineage."""
    config = get_effective_config()

    if not config['db_path']:
        print("❌ No database configured!")
        return

    from debug_engine import DebugEngine
    engine = DebugEngine(db_path=config['db_path'])

    if args.table and args.column:
        # Direct query
        report = engine.trace_column_lineage(args.table, args.column)
        print(report)

    elif args.sources:
        # Get upstream tables
        sources = engine.get_upstream_tables(args.sources)
        print(f"\n📥 Sources for {args.sources}:")
        for s in sources:
            print(f"   • {s}")
        print()

    elif args.tree:
        # Full lineage tree
        import json
        tree = engine.get_lineage_tree(args.tree)
        print(f"\n🌳 Lineage tree for {args.tree}:")
        print(json.dumps(tree, indent=2))
        print()

    else:
        # Interactive mode
        print()
        print("=" * 60)
        print("🔍 DEBUG AI - Interactive Query Mode")
        print("=" * 60)
        print()
        print("Commands:")
        print("  lineage <table> <column>  - Trace column lineage")
        print("  sources <table>           - Get upstream tables")
        print("  tree <table>              - Full lineage tree")
        print("  tables                    - List all tables")
        print("  exit                      - Quit")
        print()

        while True:
            try:
                cmd = input("debug> ").strip()

                if not cmd:
                    continue

                parts = cmd.split()
                action = parts[0].lower()

                if action in ('exit', 'quit', 'q'):
                    break

                elif action == 'lineage' and len(parts) >= 3:
                    report = engine.trace_column_lineage(parts[1], parts[2])
                    print(report)

                elif action == 'sources' and len(parts) >= 2:
                    sources = engine.get_upstream_tables(parts[1])
                    print(f"Sources: {', '.join(sources) if sources else 'None found'}")

                elif action == 'tree' and len(parts) >= 2:
                    import json
                    tree = engine.get_lineage_tree(parts[1])
                    print(json.dumps(tree, indent=2))

                elif action == 'tables':
                    tables = engine.list_tables()
                    for t in tables:
                        print(f"  {t['table_schema']}.{t['table_name']}")

                else:
                    print("Unknown command. Type 'exit' to quit.")

            except KeyboardInterrupt:
                print("\nBye!")
                break
            except Exception as e:
                print(f"Error: {e}")


def cmd_serve(args):
    """Start MCP server."""
    config = get_effective_config()

    if not config['db_path']:
        print("❌ No database configured!")
        return

    # Set environment variable for the server
    os.environ['TRACEPIPE_AI_DB_PATH'] = config['db_path']
    os.environ['DEBUG_AI_DB_PATH'] = config['db_path']  # backward compatibility

    print()
    print("=" * 60)
    print("🌐 Starting MCP Server...")
    print("=" * 60)
    print(f"\n📁 Database: {config['db_path']}")
    print()

    # Import and run server (mcp_server.py is in project root)
    sys.path.insert(0, str(PROJECT_ROOT))
    import mcp_server
    mcp_server.mcp.run()


def cmd_test(args):
    """Run all tests."""
    config = get_effective_config()

    print()
    print("=" * 60)
    print("🧪 DEBUG AI - Running Tests")
    print("=" * 60)

    # Test 1: Config
    print("\n📋 Test 1: Configuration")
    print(f"   Database: {config['db_path'] or '❌ NOT SET'}")
    print(f"   SQL Dir:  {config['sql_dir'] or '⚠️ NOT SET (optional)'}")
    print(f"   DB Type:  {config['db_type']}")

    if not config['db_path']:
        print("\n❌ Cannot continue without database!")
        print("   Run: python cli.py init --db /path/to/database.duckdb")
        return

    # Test 2: Connection
    print("\n📋 Test 2: Database Connection")
    try:
        from debug_engine import DebugEngine
        engine = DebugEngine(db_path=config['db_path'])
        print("   ✅ Connected successfully")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        return

    # Test 3: Discovery
    print("\n📋 Test 3: Table Discovery")
    try:
        schemas = engine.list_schemas()
        tables = engine.list_tables()
        print(f"   ✅ Found {len(schemas)} schemas, {len(tables)} tables")
    except Exception as e:
        print(f"   ❌ Discovery failed: {e}")

    # Test 4: Metadata
    print("\n📋 Test 4: Metadata Tables")
    meta_status = engine._check_metadata_exists()
    for name, exists in meta_status.items():
        status = "✅" if exists else "⚠️ Not found"
        print(f"   {status} {name}")

    # Test 5: Lineage Query (if metadata exists)
    if meta_status.get('column_lineage'):
        print("\n📋 Test 5: Lineage Query")
        try:
            # Get a sample
            df = engine.connector.execute(
                f"SELECT target_table, target_column FROM {engine.column_lineage_table} LIMIT 1"
            )
            if not df.empty:
                t, c = df.iloc[0]['target_table'], df.iloc[0]['target_column']
                report = engine.trace_column_lineage(t, c)
                print(f"   ✅ Successfully traced {t}.{c}")
        except Exception as e:
            print(f"   ⚠️ Could not test: {e}")

    print()
    print("=" * 60)
    print("✅ All tests completed!")
    print("=" * 60)
    print()


def cmd_peer_review(args):
    """Run senior peer review on code changes."""
    from scripts.peer_review.peer_review import PeerReviewOrchestrator
    
    orchestrator = PeerReviewOrchestrator(repo_path=args.repo)
    
    try:
        advisory = orchestrator.review_changes(staged_only=args.staged_only)
        
        print(advisory.formatted_output)
        
        # Save to file if requested
        if args.output:
            import json
            output_path = Path(args.output)
            with open(output_path, 'w') as f:
                json.dump(advisory.to_dict(), f, indent=2)
            print(f"\n💾 Detailed report saved to: {output_path}")
        
        # Exit with appropriate code for git hooks
        if args.block and advisory.risk_level == 'RED':
            print("\n🛑 Commit blocked due to RED risk level")
            print("   Use --no-verify to bypass, or fix the issues above")
            sys.exit(1)
        
        sys.exit(0)
    
    except Exception as e:
        print(f"\n❌ Error during peer review: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def cmd_peer_review_install_hook(args):
    """Install git pre-commit hook."""
    print()
    print("=" * 70)
    print("🪝 Installing Git Pre-Commit Hook")
    print("=" * 70)
    print()
    
    # Find git directory
    git_dir = PROJECT_ROOT / '.git'
    if not git_dir.exists():
        print("❌ No .git directory found. Is this a git repository?")
        return
    
    hooks_dir = git_dir / 'hooks'
    hooks_dir.mkdir(exist_ok=True)
    
    hook_file = hooks_dir / 'pre-commit'
    
    # Check if hook already exists
    if hook_file.exists() and not args.force:
        print(f"⚠️  Pre-commit hook already exists: {hook_file}")
        print("   Use --force to overwrite")
        return
    
    # Create hook script
    hook_content = f"""#!/bin/sh
# Senior Peer Review Pre-Commit Hook
# Auto-generated by Tracepipe AI

echo ""
echo "Running Senior Peer Review..."
echo ""

# Run peer review
python "{PROJECT_ROOT / 'scripts' / 'cli.py'}" peer-review check --block

# Exit with peer review result
exit $?
"""
    
    # Write hook
    hook_file.write_text(hook_content)
    
    # Make executable (on Unix-like systems)
    try:
        import stat
        hook_file.chmod(hook_file.stat().st_mode | stat.S_IEXEC)
    except:
        pass  # Windows doesn't need execute permissions
    
    print(f"✅ Pre-commit hook installed: {hook_file}")
    print()
    print("The hook will run automatically before each commit.")
    print("To bypass: git commit --no-verify")
    print()


def cmd_peer_review_uninstall_hook(args):
    """Uninstall git pre-commit hook."""
    print()
    print("Uninstalling Git Pre-Commit Hook...")
    
    git_dir = PROJECT_ROOT / '.git'
    hook_file = git_dir / 'hooks' / 'pre-commit'
    
    if hook_file.exists():
        hook_file.unlink()
        print(f"✅ Removed pre-commit hook: {hook_file}")
    else:
        print("ℹ️  No pre-commit hook found")
    print()


def cmd_peer_review_setup(args):
    """Build peer review business context (ETL discovery + lineage). Run once when installing MCP."""
    print()
    print("=" * 70)
    print("  Peer Review Setup – Building business context")
    print("=" * 70)
    print()
    try:
        from scripts.peer_review.context_builder import build_peer_review_context
        repo = getattr(args, 'repo', None) or str(PROJECT_ROOT)
        path = build_peer_review_context(repo_path=repo, run_build=True)
        print(f"  ETL files and table business logic saved to: {path}")
        print()
        print("  When you run peer review (or save changes and ask the agent to review),")
        print("  the agent will use this context and respond like a senior data engineer")
        print("  who knows your project.")
        print()
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def cmd_peer_review_config(args):
    """Configure peer review settings."""
    config_file = PROJECT_ROOT / 'config' / 'config.yml'
    
    print()
    print("=" * 70)
    print("⚙️  Peer Review Configuration")
    print("=" * 70)
    print()
    print(f"📁 Config file: {config_file}")
    print()
    
    if args.set_api_key:
        print(f"Setting GEMINI_API_KEY environment variable...")
        print()
        print("Add this to your shell profile (.bashrc, .zshrc, or PowerShell profile):")
        print()
        if os.name == 'nt':  # Windows
            print(f"  $env:GEMINI_API_KEY = '{args.set_api_key}'")
        else:  # Unix-like
            print(f"  export GEMINI_API_KEY='{args.set_api_key}'")
        print()
        return
    
    # Show current status
    print("Current Configuration:")
    print()
    
    gemini_key = os.getenv('GEMINI_API_KEY')
    if gemini_key:
        print(f"  ✅ GEMINI_API_KEY: {gemini_key[:10]}...{gemini_key[-4:]}")
    else:
        print("  ❌ GEMINI_API_KEY: Not set")
    
    print()
    print("See config/config.yml for more peer review settings")
    print()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(
        description='🔍 Tracepipe AI - Data Observability CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py init --db ./data/warehouse.duckdb --sql ./etl/
  python cli.py scan
  python cli.py build
  python cli.py query schema.table column_name
  python cli.py serve
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # INIT command
    init_parser = subparsers.add_parser('init', help='Initialize with custom paths')
    init_parser.add_argument('--db', help='Path to database file')
    init_parser.add_argument('--sql', help='Path to SQL files directory')
    init_parser.add_argument('--db-type', default='duckdb', help='Database type')
    init_parser.add_argument('--meta-schema', default='meta', help='Metadata schema name')

    # CONFIG command
    config_parser = subparsers.add_parser('config', help='Show/update configuration')
    config_parser.add_argument('--show', action='store_true', help='Show current config')
    config_parser.add_argument('--db', help='Update database path')
    config_parser.add_argument('--sql', help='Update SQL directory')
    config_parser.add_argument('--db-type', help='Update database type')
    config_parser.add_argument('--meta-schema', help='Update metadata schema')

    # SCAN command
    scan_parser = subparsers.add_parser('scan', help='Scan database structure')

    # BUILD command
    build_parser = subparsers.add_parser('build', help='Build metadata from SQL')
    build_parser.add_argument('--sql', help='SQL directory (overrides config)')

    # QUERY command
    query_parser = subparsers.add_parser('query', help='Query lineage')
    query_parser.add_argument('table', nargs='?', help='Target table')
    query_parser.add_argument('column', nargs='?', help='Target column')
    query_parser.add_argument('--sources', help='Get sources for a table')
    query_parser.add_argument('--tree', help='Get full lineage tree')

    # SERVE command
    serve_parser = subparsers.add_parser('serve', help='Start MCP server')

    # TEST command
    test_parser = subparsers.add_parser('test', help='Run all tests')
    
    # SETUP command
    setup_parser = subparsers.add_parser('setup', help='Run interactive setup wizard')
    
    # PEER-REVIEW command
    pr_parser = subparsers.add_parser('peer-review', help='Senior peer review for code changes')
    pr_subparsers = pr_parser.add_subparsers(dest='pr_command', help='Peer review sub-commands')
    
    # peer-review check
    pr_check = pr_subparsers.add_parser('check', help='Review staged/modified files')
    pr_check.add_argument('--staged-only', action='store_true', default=False, help='Only review git-staged files (default: review ALL modified files)')
    pr_check.add_argument('--output', '-o', help='Save detailed report to JSON file')
    pr_check.add_argument('--block', action='store_true', help='Exit with error on RED risk (for git hooks)')
    pr_check.add_argument('--repo', help='Path to git repository (default: current dir)')
    
    # peer-review setup
    pr_setup = pr_subparsers.add_parser('setup', help='Build business context (run once when installing MCP)')
    pr_setup.add_argument('--repo', help='Repository path (default: current dir)')
    
    # peer-review install-hook
    pr_install = pr_subparsers.add_parser('install-hook', help='Install git pre-commit hook')
    pr_install.add_argument('--force', action='store_true', help='Overwrite existing hook')
    
    # peer-review uninstall-hook
    pr_uninstall = pr_subparsers.add_parser('uninstall-hook', help='Uninstall git pre-commit hook')
    
    # peer-review config
    pr_config = pr_subparsers.add_parser('config', help='Configure peer review settings')

    args = parser.parse_args()

    if args.command == 'init':
        cmd_init(args)
    elif args.command == 'config':
        cmd_config(args)
    elif args.command == 'scan':
        cmd_scan(args)
    elif args.command == 'build':
        cmd_build(args)
    elif args.command == 'query':
        cmd_query(args)
    elif args.command == 'serve':
        cmd_serve(args)
    elif args.command == 'test':
        cmd_test(args)
    elif args.command == 'setup':
        import subprocess
        subprocess.run([sys.executable, str(SCRIPTS_DIR / 'setup_wizard.py')])
    elif args.command == 'peer-review':
        if args.pr_command == 'check':
            cmd_peer_review(args)
        elif args.pr_command == 'setup':
            cmd_peer_review_setup(args)
        elif args.pr_command == 'install-hook':
            cmd_peer_review_install_hook(args)
        elif args.pr_command == 'uninstall-hook':
            cmd_peer_review_uninstall_hook(args)
        elif args.pr_command == 'config':
            cmd_peer_review_config(args)
        else:
            pr_parser.print_help()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()