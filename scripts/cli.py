import argparse
import sys
from datetime import datetime
import json
from scripts.lineage_archive import LineageArchiver
from scripts.lineage_query import LineageQueryEngine


def cmd_archive(args):
    archiver = LineageArchiver(args.config)
    print("Extracting lineage from Databricks...")
    snapshot_id = archiver.archive_lineage()
    print(f"Archived lineage snapshot: {snapshot_id}")


def cmd_query(args):
    engine = LineageQueryEngine(args.config)
    if args.entity:
        results = engine.query_entity_lineage(
            args.entity, args.start_date, args.end_date
        )
        print(json.dumps(results, indent=2, default=str))
    elif args.timeline:
        results = engine.get_lineage_timeline(args.timeline)
        print(json.dumps(results, indent=2, default=str))
    elif args.snapshots:
        results = engine.query_snapshots(args.start_date, args.end_date)
        print(json.dumps(results, indent=2, default=str))


def cmd_audit(args):
    engine = LineageQueryEngine(args.config)
    report = engine.audit_report(args.entity, args.months)
    print(json.dumps(report, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description="Tracepipe AI CLI")
    parser.add_argument('--config', default='config/config.yml')
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    archive_parser = subparsers.add_parser('archive', help='Archive lineage data')
    archive_parser.set_defaults(func=cmd_archive)
    
    query_parser = subparsers.add_parser('query', help='Query archived lineage')
    query_parser.add_argument('--entity', help='Entity name to query')
    query_parser.add_argument('--timeline', help='Get timeline for entity')
    query_parser.add_argument('--snapshots', action='store_true', help='List snapshots')
    query_parser.add_argument('--start-date', help='Start date filter')
    query_parser.add_argument('--end-date', help='End date filter')
    query_parser.set_defaults(func=cmd_query)
    
    audit_parser = subparsers.add_parser('audit', help='Generate audit report')
    audit_parser.add_argument('entity', help='Entity to audit')
    audit_parser.add_argument('--months', type=int, default=12, help='Months back')
    audit_parser.set_defaults(func=cmd_audit)
    
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
