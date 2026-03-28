#!/usr/bin/env python3
"""
TracePipe AI CLI

Command-line interface for TracePipe AI data observability features.
"""

import sys
import os
import argparse
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.impact_analysis.lineage_graph import LineageGraphBuilder
from scripts.impact_analysis.impact_analyzer import ImpactAnalyzer
from scripts.impact_analysis.root_cause_analyzer import RootCauseAnalyzer
from scripts.impact_analysis.visualizer import DependencyVisualizer


def get_default_db_path():
    """Get default database path"""
    current_dir = Path(__file__).parent.parent
    db_path = current_dir / "companies_data_duckdb" / "corporate.duckdb"
    return str(db_path)


def cmd_impact_analysis(args):
    """Run impact analysis (blast radius)"""
    print(f"\n🔍 Analyzing impact for: {args.asset}")
    print(f"Database: {args.db_path}\n")
    
    # Build lineage graph
    print("Building lineage graph...")
    builder = LineageGraphBuilder(args.db_path)
    graph = builder.build_graph()
    print(f"✓ Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges\n")
    
    # Run impact analysis
    analyzer = ImpactAnalyzer(graph)
    
    try:
        if args.simulate:
            # Simulation mode
            result = analyzer.simulate_change(args.asset, args.change_type)
            print(json.dumps(result, indent=2))
        else:
            # Standard impact analysis
            result = analyzer.analyze_impact(args.asset, args.max_depth)
            
            # Visualize results
            visualizer = DependencyVisualizer(graph)
            all_affected = result.affected_tables + result.affected_views + result.affected_columns
            output = visualizer.visualize_impact(result.source_asset, all_affected, result.impact_paths)
            print(output)
            
            # Print summary
            print(f"\nSUMMARY:")
            print(f"  Affected tables: {len(result.affected_tables)}")
            print(f"  Affected views: {len(result.affected_views)}")
            print(f"  Affected columns: {len(result.affected_columns)}")
            print(f"  Total affected: {result.total_affected}")
            print(f"  Maximum depth: {result.impact_depth}\n")
            
            if args.json:
                output_data = {
                    'source_asset': result.source_asset,
                    'affected_tables': result.affected_tables,
                    'affected_views': result.affected_views,
                    'affected_columns': result.affected_columns[:20],
                    'total_affected': result.total_affected,
                    'impact_depth': result.impact_depth
                }
                print("\nJSON OUTPUT:")
                print(json.dumps(output_data, indent=2))
    
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_root_cause(args):
    """Run root cause analysis"""
    print(f"\n🔍 Analyzing root cause for: {args.asset}")
    print(f"Database: {args.db_path}\n")
    
    # Build lineage graph
    print("Building lineage graph...")
    builder = LineageGraphBuilder(args.db_path)
    graph = builder.build_graph()
    print(f"✓ Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges\n")
    
    # Run root cause analysis
    analyzer = RootCauseAnalyzer(graph)
    
    try:
        if args.diagnose:
            # Diagnosis mode for data quality issues
            result = analyzer.diagnose_data_quality_issue(args.asset)
            print(json.dumps(result, indent=2))
        else:
            # Standard root cause analysis
            result = analyzer.analyze_root_cause(args.asset, args.max_depth)
            
            # Visualize results
            visualizer = DependencyVisualizer(graph)
            all_sources = result.source_tables + result.source_columns
            output = visualizer.visualize_root_cause(result.target_asset, all_sources, result.dependency_paths)
            print(output)
            
            # Print critical dependencies
            if result.critical_dependencies:
                print("CRITICAL DEPENDENCIES (direct sources):")
                for dep in result.critical_dependencies:
                    print(f"  • {dep}")
                print()
            
            # Print summary
            print(f"SUMMARY:")
            print(f"  Source tables: {len(result.source_tables)}")
            print(f"  Source columns: {len(result.source_columns)}")
            print(f"  Total dependencies: {result.total_dependencies}")
            print(f"  Maximum depth: {result.dependency_depth}\n")
            
            if args.json:
                output_data = {
                    'target_asset': result.target_asset,
                    'source_tables': result.source_tables,
                    'source_columns': result.source_columns[:20],
                    'critical_dependencies': result.critical_dependencies,
                    'total_dependencies': result.total_dependencies,
                    'dependency_depth': result.dependency_depth
                }
                print("\nJSON OUTPUT:")
                print(json.dumps(output_data, indent=2))
    
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_lineage_graph(args):
    """Display lineage graph summary"""
    print(f"\n📊 Building lineage graph...")
    print(f"Database: {args.db_path}\n")
    
    # Build lineage graph
    builder = LineageGraphBuilder(args.db_path)
    graph = builder.build_graph()
    
    # Visualize summary
    visualizer = DependencyVisualizer(graph)
    print(visualizer.visualize_graph_summary())
    
    if args.export:
        output_file = args.export
        result = visualizer.export_to_dot(output_file)
        print(result)
        print()


def main():
    parser = argparse.ArgumentParser(
        description='TracePipe AI - Data Observability CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Impact analysis command
    impact_parser = subparsers.add_parser(
        'impact',
        help='Analyze downstream impact (blast radius) of changes'
    )
    impact_parser.add_argument('asset', help='Asset identifier (table, view, or column)')
    impact_parser.add_argument('--db-path', default=get_default_db_path(), help='Path to DuckDB database')
    impact_parser.add_argument('--max-depth', type=int, default=10, help='Maximum dependency depth')
    impact_parser.add_argument('--simulate', action='store_true', help='Simulate change impact')
    impact_parser.add_argument('--change-type', default='schema', choices=['schema', 'data', 'removal'], 
                             help='Type of change to simulate')
    impact_parser.add_argument('--json', action='store_true', help='Output results as JSON')
    impact_parser.set_defaults(func=cmd_impact_analysis)
    
    # Root cause analysis command
    rootcause_parser = subparsers.add_parser(
        'rootcause',
        help='Analyze upstream dependencies for root cause identification'
    )
    rootcause_parser.add_argument('asset', help='Asset identifier (table, view, or column)')
    rootcause_parser.add_argument('--db-path', default=get_default_db_path(), help='Path to DuckDB database')
    rootcause_parser.add_argument('--max-depth', type=int, default=10, help='Maximum dependency depth')
    rootcause_parser.add_argument('--diagnose', action='store_true', help='Diagnose data quality issues')
    rootcause_parser.add_argument('--json', action='store_true', help='Output results as JSON')
    rootcause_parser.set_defaults(func=cmd_root_cause)
    
    # Lineage graph command
    graph_parser = subparsers.add_parser(
        'lineage',
        help='Display lineage graph summary'
    )
    graph_parser.add_argument('--db-path', default=get_default_db_path(), help='Path to DuckDB database')
    graph_parser.add_argument('--export', help='Export graph to DOT file')
    graph_parser.set_defaults(func=cmd_lineage_graph)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    args.func(args)


if __name__ == '__main__':
    main()
