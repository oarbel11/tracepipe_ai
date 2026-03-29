import argparse
import sys
from datetime import datetime, timedelta
import pandas as pd
from scripts.lineage_history import LineageHistoryStore


def export_lineage_command(args):
    store = LineageHistoryStore(args.db_path)
    
    if args.source_csv:
        df = pd.read_csv(args.source_csv)
    else:
        df = pd.DataFrame({
            'source_table': ['catalog.schema.table1'],
            'target_table': ['catalog.schema.table2'],
            'source_column': ['col1'],
            'target_column': ['col2'],
            'lineage_type': ['table']
        })
    
    count = store.export_lineage(df)
    print(f"Exported {count} lineage records")
    store.close()


def time_travel_command(args):
    store = LineageHistoryStore(args.db_path)
    target_date = datetime.fromisoformat(args.date)
    
    result = store.time_travel(target_date, args.table)
    print(f"Found {len(result)} lineage records as of {args.date}")
    print(result.to_string())
    store.close()


def lineage_evolution_command(args):
    store = LineageHistoryStore(args.db_path)
    start = datetime.fromisoformat(args.start_date)
    end = datetime.fromisoformat(args.end_date)
    
    evolution = store.get_lineage_evolution(args.table, start, end)
    print(f"Lineage evolution for {args.table}:")
    for entry in evolution:
        print(f"  {entry['timestamp']}: {entry['source']} -> {entry['target']}")
    store.close()


def main():
    parser = argparse.ArgumentParser(description="Tracepipe AI CLI")
    subparsers = parser.add_subparsers(dest='command')
    
    export_parser = subparsers.add_parser('export-lineage')
    export_parser.add_argument('--source-csv', help='CSV with lineage data')
    export_parser.add_argument('--db-path', default='lineage_history.duckdb')
    
    travel_parser = subparsers.add_parser('time-travel')
    travel_parser.add_argument('--date', required=True, help='ISO format date')
    travel_parser.add_argument('--table', help='Filter by table name')
    travel_parser.add_argument('--db-path', default='lineage_history.duckdb')
    
    evolution_parser = subparsers.add_parser('lineage-evolution')
    evolution_parser.add_argument('--table', required=True)
    evolution_parser.add_argument('--start-date', required=True)
    evolution_parser.add_argument('--end-date', required=True)
    evolution_parser.add_argument('--db-path', default='lineage_history.duckdb')
    
    args = parser.parse_args()
    
    if args.command == 'export-lineage':
        export_lineage_command(args)
    elif args.command == 'time-travel':
        time_travel_command(args)
    elif args.command == 'lineage-evolution':
        lineage_evolution_command(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
