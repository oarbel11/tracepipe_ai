#!/usr/bin/env python3
"""CLI for Tracepipe AI operations."""
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tracepipe_ai.lineage_archive import LineageArchive


def archive_lineage_cmd(args):
    """Archive lineage data from input file or mock data."""
    archive = LineageArchive()
    
    if len(args) > 0 and Path(args[0]).exists():
        with open(args[0], 'r') as f:
            data = json.load(f)
    else:
        data = [
            {
                'event_time': datetime.now().isoformat(),
                'source_table': 'catalog.schema.source',
                'target_table': 'catalog.schema.target',
                'operation': 'INSERT',
                'metadata': {'user': 'system', 'query_id': '123'}
            }
        ]
    
    count = archive.archive_lineage(data)
    archive.close()
    print(f"Archived {count} lineage events")
    return 0


def query_lineage_cmd(args):
    """Query historical lineage data."""
    archive = LineageArchive()
    
    end = datetime.now()
    start = end - timedelta(days=365)
    table = args[0] if len(args) > 0 else None
    
    results = archive.query_historical(
        start.isoformat(), end.isoformat(), table
    )
    archive.close()
    
    print(json.dumps(results, indent=2, default=str))
    return 0


def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: cli.py [archive|query] [args...]")
        return 1
    
    cmd = sys.argv[1]
    args = sys.argv[2:]
    
    if cmd == 'archive':
        return archive_lineage_cmd(args)
    elif cmd == 'query':
        return query_lineage_cmd(args)
    else:
        print(f"Unknown command: {cmd}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
