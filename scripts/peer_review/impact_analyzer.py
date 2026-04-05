"""Interactive Impact Analysis & Governance Policy Overlay."""

import networkx as nx
from typing import Dict, List, Set, Any, Optional


class InteractiveImpactAnalyzer:
    """Analyzes impact of changes on downstream assets with governance overlay."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.policies: Dict[str, Dict[str, Any]] = {}

    def add_asset(self, asset_id: str, metadata: Dict[str, Any]):
        """Add an asset to the lineage graph."""
        self.graph.add_node(asset_id, **metadata)

    def add_dependency(self, source_id: str, target_id: str):
        """Add a dependency edge between assets."""
        self.graph.add_edge(source_id, target_id)

    def add_governance_policy(self, policy_id: str, policy_data: Dict[str, Any]):
        """Add a governance policy."""
        self.policies[policy_id] = policy_data

    def analyze_impact(self, asset_id: str, filters: Optional[Dict] = None) -> Dict:
        """Analyze downstream impact of an asset with optional filters."""
        if asset_id not in self.graph:
            return {"error": f"Asset {asset_id} not found", "impacted_assets": []}

        filters = filters or {}
        descendants = nx.descendants(self.graph, asset_id)
        impacted = [asset_id] + list(descendants)

        filtered_assets = self._apply_filters(impacted, filters)
        policies = self._get_applicable_policies(filtered_assets)

        return {
            "source_asset": asset_id,
            "impacted_assets": filtered_assets,
            "total_count": len(filtered_assets),
            "governance_policies": policies
        }

    def _apply_filters(self, assets: List[str], filters: Dict) -> List[str]:
        """Apply filters to asset list."""
        result = []
        for asset in assets:
            if asset not in self.graph:
                continue
            metadata = self.graph.nodes[asset]
            if self._matches_filters(metadata, filters):
                result.append(asset)
        return result

    def _matches_filters(self, metadata: Dict, filters: Dict) -> bool:
        """Check if metadata matches all filters."""
        for key, value in filters.items():
            if key == "tags" and value:
                asset_tags = metadata.get("tags", [])
                if not any(tag in asset_tags for tag in value):
                    return False
            elif key in metadata and metadata[key] != value:
                return False
        return True

    def _get_applicable_policies(self, assets: List[str]) -> List[Dict]:
        """Get governance policies applicable to assets."""
        applicable = []
        for policy_id, policy_data in self.policies.items():
            target_assets = policy_data.get("target_assets", [])
            if any(asset in target_assets for asset in assets):
                applicable.append({"policy_id": policy_id, **policy_data})
        return applicable
