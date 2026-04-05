import sys
import json
import argparse
from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer
from scripts.peer_review.governance_policy import GovernancePolicy

def run_impact_analysis(args):
    """Run impact analysis command."""
    analyzer = InteractiveImpactAnalyzer()
    
    if args.lineage_file:
        with open(args.lineage_file, 'r') as f:
            lineage_data = json.load(f)
            for asset in lineage_data.get("assets", []):
                analyzer.add_asset(asset["id"], asset.get("metadata", {}))
            for dep in lineage_data.get("dependencies", []):
                analyzer.add_dependency(dep["source"], dep["target"])
    
    if args.policy_file:
        with open(args.policy_file, 'r') as f:
            policies_data = json.load(f)
            for p in policies_data.get("policies", []):
                policy = GovernancePolicy(**p)
                analyzer.add_policy(policy)
    
    filters = {}
    if args.tags:
        filters["tags"] = args.tags.split(",")
    if args.owner:
        filters["owner"] = args.owner
    if args.quality_status:
        filters["quality_status"] = args.quality_status
    
    result = analyzer.analyze_impact(args.asset_id, filters)
    print(json.dumps(result, indent=2))

def main():
    parser = argparse.ArgumentParser(description="Tracepipe AI CLI")
    subparsers = parser.add_subparsers(dest="command")
    
    impact_parser = subparsers.add_parser("impact-analysis")
    impact_parser.add_argument("asset_id", help="Asset ID to analyze")
    impact_parser.add_argument("--lineage-file", help="JSON file with lineage")
    impact_parser.add_argument("--policy-file", help="JSON file with policies")
    impact_parser.add_argument("--tags", help="Filter by tags (comma-separated)")
    impact_parser.add_argument("--owner", help="Filter by owner")
    impact_parser.add_argument("--quality-status", help="Filter by quality status")
    
    args = parser.parse_args()
    
    if args.command == "impact-analysis":
        run_impact_analysis(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
