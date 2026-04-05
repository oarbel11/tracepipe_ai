import argparse
import sys
import json
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='Tracepipe AI CLI')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    trace_parser = subparsers.add_parser('trace-transformations',
                                         help='Trace column transformations')
    trace_parser.add_argument('--file', required=True,
                             help='SQL file to analyze')
    trace_parser.add_argument('--output', default='lineage.json',
                             help='Output file')
    trace_parser.add_argument('--format', choices=['json', 'mermaid'],
                             default='json', help='Output format')
    trace_parser.add_argument('--source-table', help='Source table name')
    
    setup_parser = subparsers.add_parser('setup', help='Setup wizard')
    
    args = parser.parse_args()
    
    if args.command == 'trace-transformations':
        trace_transformations(args)
    elif args.command == 'setup':
        from scripts.setup_wizard import run_setup
        run_setup()
    else:
        parser.print_help()

def trace_transformations(args):
    from scripts.transformation_tracer import TransformationTracer
    from scripts.transformation_visualizer import TransformationVisualizer
    
    sql_file = Path(args.file)
    if not sql_file.exists():
        print(f"Error: File {args.file} not found")
        sys.exit(1)
    
    sql_code = sql_file.read_text()
    
    tracer = TransformationTracer()
    lineage = tracer.trace_transformations(sql_code, args.source_table)
    
    visualizer = TransformationVisualizer(lineage.graph)
    
    if args.format == 'json':
        output = visualizer.to_json()
    else:
        output = visualizer.to_mermaid()
    
    Path(args.output).write_text(output)
    
    summary = visualizer.get_transformation_summary()
    print(f"Traced {summary['total_columns']} columns with "
          f"{summary['total_transformations']} transformations")
    print(f"Output written to {args.output}")

if __name__ == '__main__':
    main()
