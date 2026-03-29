import argparse
import sys
import logging
from .databricks_lineage.lineage_extractor import DatabricksLineageExtractor
from .databricks_lineage.lineage_graph import LineageGraphBuilder

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def extract_databricks_lineage(args):
    """Extract and build Databricks end-to-end lineage graph."""
    extractor = DatabricksLineageExtractor(
        host=args.host,
        token=args.token,
        http_path=args.http_path
    )
    
    logger.info("Extracting table lineage from Unity Catalog...")
    table_lineage = extractor.extract_table_lineage()
    
    logger.info("Extracting jobs metadata...")
    jobs = extractor.extract_jobs()
    
    logger.info("Extracting DLT pipelines...")
    dlt_pipelines = extractor.extract_dlt_pipelines()
    
    logger.info("Building lineage graph...")
    graph_builder = LineageGraphBuilder()
    graph_builder.add_table_lineage(table_lineage)
    graph_builder.add_job_lineage(jobs)
    graph_builder.add_dlt_lineage(dlt_pipelines)
    
    stats = graph_builder.get_stats()
    logger.info(f"Lineage Graph Stats: {stats}")
    
    output_path = args.output or 'databricks_lineage.json'
    graph_builder.export_json(output_path)
    logger.info(f"Lineage graph exported to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Tracepipe AI CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    lineage_parser = subparsers.add_parser('extract-lineage', 
                                           help='Extract Databricks end-to-end lineage')
    lineage_parser.add_argument('--host', required=True, help='Databricks workspace URL')
    lineage_parser.add_argument('--token', required=True, help='Databricks access token')
    lineage_parser.add_argument('--http-path', help='SQL warehouse HTTP path for Unity Catalog queries')
    lineage_parser.add_argument('--output', help='Output JSON file path')
    
    args = parser.parse_args()
    
    if args.command == 'extract-lineage':
        extract_databricks_lineage(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
