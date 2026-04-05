import argparse
import sys
import json
from scripts.bi_integration import BIIntegrationEngine

def bi_sync_command(args):
    engine = BIIntegrationEngine()
    count = engine.sync_bi_metadata(args.platform, args.workspace)
    print(f"Synced {count} metrics from {args.platform}")
    return 0

def bi_trace_command(args):
    engine = BIIntegrationEngine()
    engine.sync_bi_metadata('tableau', 'default')
    lineage = engine.trace_metric_to_source(args.metric, args.dashboard)
    print(json.dumps(lineage, indent=2))
    return 0

def bi_list_command(args):
    engine = BIIntegrationEngine()
    engine.sync_bi_metadata('tableau', 'default')
    metrics = engine.get_all_metrics()
    for m in metrics:
        print(f"{m['platform']}: {m['dashboard']} - {m['name']}")
    return 0

def bi_validate_command(args):
    engine = BIIntegrationEngine()
    engine.sync_bi_metadata('tableau', 'default')
    results = engine.validate_all_metrics()
    print(f"Valid metrics: {len(results['valid'])}")
    print(f"Invalid metrics: {len(results['invalid'])}")
    if results['invalid']:
        print("Invalid:", results['invalid'])
    return 0

def main():
    parser = argparse.ArgumentParser(description='TracePipe AI CLI')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    sync_parser = subparsers.add_parser('bi-sync', help='Sync BI metadata')
    sync_parser.add_argument('--platform', required=True, help='BI platform')
    sync_parser.add_argument('--workspace', required=True, help='Workspace')

    trace_parser = subparsers.add_parser('bi-trace', help='Trace metric')
    trace_parser.add_argument('--metric', required=True, help='Metric name')
    trace_parser.add_argument('--dashboard', help='Dashboard name')

    subparsers.add_parser('bi-list', help='List all BI metrics')
    subparsers.add_parser('bi-validate', help='Validate all metrics')

    args = parser.parse_args()
    if args.command == 'bi-sync':
        return bi_sync_command(args)
    elif args.command == 'bi-trace':
        return bi_trace_command(args)
    elif args.command == 'bi-list':
        return bi_list_command(args)
    elif args.command == 'bi-validate':
        return bi_validate_command(args)
    else:
        parser.print_help()
        return 1

if __name__ == '__main__':
    sys.exit(main())
