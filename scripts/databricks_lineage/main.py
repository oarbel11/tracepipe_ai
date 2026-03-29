"""Main script for Databricks lineage extraction."""

import os
import sys
import argparse
from .lineage_extractor import DatabricksLineageExtractor
from .visualizer import LineageVisualizer


def main():
    """Main entry point for lineage extraction."""
    parser = argparse.ArgumentParser(
        description='Extract Databricks pipeline lineage'
    )
    parser.add_argument(
        '--host',
        default=os.getenv('DATABRICKS_HOST'),
        help='Databricks workspace URL'
    )
    parser.add_argument(
        '--token',
        default=os.getenv('DATABRICKS_TOKEN'),
        help='Databricks access token'
    )
    parser.add_argument(
        '--output',
        default='lineage.json',
        help='Output file path'
    )
    parser.add_argument(
        '--format',
        choices=['json', 'ascii'],
        default='json',
        help='Output format'
    )
    
    args = parser.parse_args()
    
    if not args.host or not args.token:
        print("Error: DATABRICKS_HOST and DATABRICKS_TOKEN required")
        sys.exit(1)
    
    extractor = DatabricksLineageExtractor(args.host, args.token)
    lineage = extractor.extract_lineage()
    
    visualizer = LineageVisualizer(lineage)
    visualizer.save_to_file(args.output, args.format)
    
    stats = visualizer.get_statistics()
    print(f"Lineage extracted: {stats}")
    print(f"Output saved to: {args.output}")


if __name__ == '__main__':
    main()
