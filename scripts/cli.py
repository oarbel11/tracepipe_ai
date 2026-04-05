import argparse
import sys
from scripts.operational_lineage import OperationalLineageTracker


def main():
    parser = argparse.ArgumentParser(description='Tracepipe AI CLI')
    subparsers = parser.add_subparsers(dest='command')
    
    lineage_parser = subparsers.add_parser('lineage', help='Capture operational lineage')
    lineage_parser.add_argument('--catalog', required=True, help='Catalog name')
    lineage_parser.add_argument('--days', type=int, default=7, help='Days to look back')
    lineage_parser.add_argument('--output', default='lineage.html', help='Output file')
    
    trace_parser = subparsers.add_parser('trace', help='Trace upstream code for a table')
    trace_parser.add_argument('--table', required=True, help='Fully qualified table name')
    
    args = parser.parse_args()
    
    if args.command == 'lineage':
        tracker = OperationalLineageTracker()
        tracker.capture_lineage(catalog=args.catalog, days_back=args.days)
        tracker.visualize_graph(args.output)
        print(f"Lineage graph saved to {args.output}")
    
    elif args.command == 'trace':
        tracker = OperationalLineageTracker()
        tracker.capture_lineage(catalog=args.table.split('.')[0], days_back=7)
        upstream = tracker.get_upstream_code(args.table)
        if upstream:
            print(f"Upstream code for {args.table}:")
            for code in upstream:
                print(f"  - {code.get('statement_type')} by {code.get('user')} at {code.get('timestamp')}")
        else:
            print(f"No upstream code found for {args.table}")
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
