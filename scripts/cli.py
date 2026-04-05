import argparse
import sys
from .peer_review.impact_analyzer import InteractiveImpactAnalyzer
from .peer_review.governance import GovernancePolicy


def impact_analysis_command(args):
    analyzer = InteractiveImpactAnalyzer()
    
    # Simulate loading lineage (in production, load from metadata store)
    analyzer.add_asset("companies_data.main.customers", {
        "tags": ["PII", "customer"],
        "owner": "data-team",
        "quality_status": "healthy"
    })
    analyzer.add_asset("companies_data.main.orders", {
        "tags": ["financial"],
        "owner": "analytics-team",
        "quality_status": "degraded"
    })
    analyzer.add_asset("companies_data.main.customer_360", {
        "tags": ["PII", "analytics"],
        "owner": "data-team",
        "quality_status": "healthy"
    })
    analyzer.add_dependency("companies_data.main.customers", "companies_data.main.customer_360")
    analyzer.add_dependency("companies_data.main.orders", "companies_data.main.customer_360")
    
    filters = {}
    if args.filter_tag:
        filters["tags"] = [args.filter_tag]
    if args.filter_owner:
        filters["owner"] = args.filter_owner
    if args.filter_quality:
        filters["quality_status"] = args.filter_quality
    
    result = analyzer.analyze_impact(
        asset_name=args.asset,
        filters=filters,
        max_depth=args.depth
    )
    
    print(result.to_json())


def main():
    parser = argparse.ArgumentParser(description="Tracepipe AI CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    impact_parser = subparsers.add_parser("impact-analysis", help="Analyze impact of changes")
    impact_parser.add_argument("--asset", required=True, help="Asset name to analyze")
    impact_parser.add_argument("--filter-tag", help="Filter by tag")
    impact_parser.add_argument("--filter-owner", help="Filter by owner")
    impact_parser.add_argument("--filter-quality", help="Filter by quality status")
    impact_parser.add_argument("--depth", type=int, default=None, help="Max traversal depth")
    
    args = parser.parse_args()
    
    if args.command == "impact-analysis":
        impact_analysis_command(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
