import argparse
import sys
import json
from scripts.debug_engine import DebugEngine
from scripts.build_metadata import MetadataBuilder
from scripts.operational_lineage import OperationalLineageExtractor
from config.db_config import get_databricks_config


def debug_command(args):
    engine = DebugEngine()
    result = engine.diagnose(args.query, args.verbose)
    print(json.dumps(result, indent=2))


def metadata_command(args):
    builder = MetadataBuilder()
    metadata = builder.extract_all(args.catalog, args.schema)
    print(json.dumps(metadata, indent=2))


def lineage_command(args):
    """Extract and display operational lineage."""
    config = get_databricks_config()
    extractor = OperationalLineageExtractor(config)
    graph = extractor.build_lineage_graph()
    
    if args.table:
        upstream = extractor.get_upstream_code(args.table)
        print(f"Code assets producing {args.table}:")
        for code in upstream:
            print(f"  - {code}")
    elif args.code:
        downstream = extractor.get_downstream_impact(args.code)
        print(f"Tables affected by {args.code}:")
        for table in downstream:
            print(f"  - {table}")
    else:
        stats = {
            "total_nodes": graph.number_of_nodes(),
            "total_edges": graph.number_of_edges(),
            "code_assets": len([n for n, d in graph.nodes(data=True) 
                                if d['type'] in ['notebook', 'job']]),
            "data_assets": len([n for n, d in graph.nodes(data=True) 
                               if d['type'] == 'table'])
        }
        print(json.dumps(stats, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Tracepipe AI CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    debug_parser = subparsers.add_parser("debug", help="Debug SQL queries")
    debug_parser.add_argument("query", help="SQL query to debug")
    debug_parser.add_argument("-v", "--verbose", action="store_true")

    metadata_parser = subparsers.add_parser("metadata", help="Build metadata")
    metadata_parser.add_argument("--catalog", default="main")
    metadata_parser.add_argument("--schema", default="default")

    lineage_parser = subparsers.add_parser("lineage", help="Operational lineage")
    lineage_parser.add_argument("--table", help="Find code producing table")
    lineage_parser.add_argument("--code", help="Find tables affected by code")

    args = parser.parse_args()

    if args.command == "debug":
        debug_command(args)
    elif args.command == "metadata":
        metadata_command(args)
    elif args.command == "lineage":
        lineage_command(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
