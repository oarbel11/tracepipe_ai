"""CLI commands for impact and root cause analysis."""

import json
import sys
from typing import Optional
from src.impact_analysis import ImpactAnalysisEngine, DataAsset


def get_db_connection():
    """Get database connection if available."""
    try:
        import duckdb
        return duckdb.connect(":memory:")
    except ImportError:
        return None


def analyze_impact_command(asset_id: str, output_format: str = "text") -> int:
    """CLI command to analyze downstream impact.
    
    Args:
        asset_id: ID of the asset to analyze
        output_format: Output format (text, json)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        engine = ImpactAnalysisEngine(get_db_connection())
        engine.load_metadata()
        
        result = engine.analyze_downstream_impact(asset_id)
        
        if output_format == "json":
            output = {
                "root_asset": {
                    "id": result.root_asset.asset_id,
                    "type": result.root_asset.asset_type,
                    "name": result.root_asset.name
                },
                "affected_count": len(result.affected_assets),
                "affected_assets": [
                    {"id": a.asset_id, "type": a.asset_type, "name": a.name}
                    for a in result.affected_assets
                ],
                "impact_score": result.impact_score,
                "recommendations": result.recommendations
            }
            print(json.dumps(output, indent=2))
        else:
            print(f"\nImpact Analysis for: {result.root_asset.name}")
            print(f"Asset Type: {result.root_asset.asset_type}")
            print(f"\nAffected Assets: {len(result.affected_assets)}")
            print(f"Impact Score: {result.impact_score:.2f}\n")
            
            if result.affected_assets:
                print("Downstream Dependencies:")
                for asset in result.affected_assets[:10]:
                    print(f"  - {asset.asset_id} ({asset.asset_type})")
                if len(result.affected_assets) > 10:
                    print(f"  ... and {len(result.affected_assets) - 10} more")
            
            print("\nRecommendations:")
            for rec in result.recommendations:
                print(f"  - {rec}")
        
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def analyze_root_cause_command(asset_id: str, output_format: str = "text") -> int:
    """CLI command to analyze upstream root causes.
    
    Args:
        asset_id: ID of the affected asset
        output_format: Output format (text, json)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        engine = ImpactAnalysisEngine(get_db_connection())
        engine.load_metadata()
        
        result = engine.analyze_root_cause(asset_id)
        
        if output_format == "json":
            output = {
                "affected_asset": {
                    "id": result.affected_asset.asset_id,
                    "type": result.affected_asset.asset_type,
                    "name": result.affected_asset.name
                },
                "root_causes_count": len(result.root_causes),
                "root_causes": [
                    {"id": a.asset_id, "type": a.asset_type, "name": a.name}
                    for a in result.root_causes
                ],
                "confidence_score": result.confidence_score
            }
            print(json.dumps(output, indent=2))
        else:
            print(f"\nRoot Cause Analysis for: {result.affected_asset.name}")
            print(f"Asset Type: {result.affected_asset.asset_type}")
            print(f"\nPotential Root Causes: {len(result.root_causes)}")
            print(f"Confidence Score: {result.confidence_score:.2f}\n")
            
            if result.root_causes:
                print("Upstream Dependencies:")
                for asset in result.root_causes:
                    print(f"  - {asset.asset_id} ({asset.asset_type})")
            else:
                print("No upstream dependencies found (this may be a root asset)")
        
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def show_lineage_command(asset_id: str, output_format: str = "text") -> int:
    """CLI command to show complete lineage (upstream and downstream).
    
    Args:
        asset_id: ID of the asset
        output_format: Output format (text, json)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        engine = ImpactAnalysisEngine(get_db_connection())
        engine.load_metadata()
        
        lineage = engine.get_asset_lineage(asset_id)
        
        if output_format == "json":
            print(json.dumps(lineage, indent=2))
        else:
            asset = lineage["asset"]
            print(f"\nLineage for: {asset['name']}")
            print(f"Asset Type: {asset['asset_type']}")
            print(f"Asset ID: {asset['asset_id']}\n")
            
            print(f"Upstream Dependencies: {lineage['upstream']['count']}")
            for a in lineage['upstream']['assets'][:5]:
                print(f"  - {a['asset_id']} ({a['asset_type']})")
            
            print(f"\nDownstream Dependencies: {lineage['downstream']['count']}")
            for a in lineage['downstream']['assets'][:5]:
                print(f"  - {a['asset_id']} ({a['asset_type']})")
        
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    # Simple CLI interface
    if len(sys.argv) < 3:
        print("Usage: python cli_impact_analysis.py <command> <asset_id> [format]")
        print("Commands: impact, root-cause, lineage")
        sys.exit(1)
    
    command = sys.argv[1]
    asset_id = sys.argv[2]
    output_format = sys.argv[3] if len(sys.argv) > 3 else "text"
    
    if command == "impact":
        sys.exit(analyze_impact_command(asset_id, output_format))
    elif command == "root-cause":
        sys.exit(analyze_root_cause_command(asset_id, output_format))
    elif command == "lineage":
        sys.exit(show_lineage_command(asset_id, output_format))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
