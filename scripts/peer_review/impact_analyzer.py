"""Interactive Impact Analysis for Tracepipe AI."""
import json
from typing import Dict, List, Set, Any, Optional
import networkx as nx


class InteractiveImpactAnalyzer:
    """Analyzes impact and blast radius of changes in data lineage."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.metadata = {}
        self.policies = {}

    def add_asset(self, asset_id: str, metadata: Dict[str, Any]) -> None:
        """Add an asset with metadata."""
        self.graph.add_node(asset_id)
        self.metadata[asset_id] = metadata

    def add_dependency(self, source: str, target: str) -> None:
        """Add a dependency relationship."""
        self.graph.add_edge(source, target)

    def add_policy(self, policy_id: str, policy_data: Dict[str, Any]) -> None:
        """Add a governance policy."""
        self.policies[policy_id] = policy_data

    def analyze_impact(self, asset_id: str, filters: Optional[Dict] = None
                       ) -> Dict[str, Any]:
        """Analyze downstream impact of an asset."""
        if asset_id not in self.graph:
            return {"downstream": [], "count": 0, "policies": []}

        downstream = list(nx.descendants(self.graph, asset_id))
        filtered = self._apply_filters(downstream, filters or {})
        applicable_policies = self._get_applicable_policies(asset_id, filtered)

        return {
            "downstream": filtered,
            "count": len(filtered),
            "policies": applicable_policies
        }

    def _apply_filters(self, assets: List[str], filters: Dict) -> List[str]:
        """Apply filters to asset list."""
        result = []
        for asset in assets:
            meta = self.metadata.get(asset, {})
            if self._matches_filters(meta, filters):
                result.append(asset)
        return result

    def _matches_filters(self, metadata: Dict, filters: Dict) -> bool:
        """Check if metadata matches all filters."""
        for key, value in filters.items():
            if key == "tags" and isinstance(value, list):
                asset_tags = metadata.get("tags", [])
                if not any(tag in asset_tags for tag in value):
                    return False
            elif metadata.get(key) != value:
                return False
        return True

    def _get_applicable_policies(self, asset_id: str, downstream: List[str]
                                  ) -> List[Dict]:
        """Get policies applicable to asset and downstream."""
        result = []
        all_assets = [asset_id] + downstream
        for policy_id, policy in self.policies.items():
            if self._policy_applies(policy, all_assets):
                result.append({"id": policy_id, **policy})
        return result

    def _policy_applies(self, policy: Dict, assets: List[str]) -> bool:
        """Check if policy applies to any asset."""
        target_tags = policy.get("target_tags", [])
        for asset in assets:
            meta = self.metadata.get(asset, {})
            asset_tags = meta.get("tags", [])
            if any(tag in asset_tags for tag in target_tags):
                return True
        return False
