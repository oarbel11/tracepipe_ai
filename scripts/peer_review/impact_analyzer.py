"""Interactive impact analysis for data lineage."""
import json
from typing import Dict, List, Set, Optional, Any
from collections import deque


class InteractiveImpactAnalyzer:
    """Analyzes impact of changes across data lineage."""

    def __init__(self):
        self.lineage_graph: Dict[str, List[str]] = {}
        self.asset_metadata: Dict[str, Dict[str, Any]] = {}
        self.governance_policies: Dict[str, List[Dict[str, Any]]] = {}

    def load_lineage(self, lineage_data: Dict[str, Any]) -> None:
        """Load lineage graph from data."""
        self.lineage_graph = {}
        edges = lineage_data.get("edges", [])
        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if source and target:
                if source not in self.lineage_graph:
                    self.lineage_graph[source] = []
                self.lineage_graph[source].append(target)

        nodes = lineage_data.get("nodes", [])
        for node in nodes:
            node_id = node.get("id")
            if node_id:
                self.asset_metadata[node_id] = node

    def analyze_downstream_impact(self, asset_id: str,
                                  filters: Optional[Dict] = None) -> Dict:
        """Analyze downstream impact of an asset change."""
        if asset_id not in self.asset_metadata:
            return {"error": "Asset not found", "impacted_assets": []}

        impacted = self._get_downstream_assets(asset_id)
        filtered = self._apply_filters(impacted, filters or {})

        return {
            "source_asset": asset_id,
            "total_impacted": len(filtered),
            "impacted_assets": filtered,
            "policies": self._get_applicable_policies(filtered)
        }

    def _get_downstream_assets(self, asset_id: str) -> List[str]:
        """Get all downstream assets using BFS."""
        visited: Set[str] = set()
        queue = deque([asset_id])
        downstream = []

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)

            if current != asset_id:
                downstream.append(current)

            for neighbor in self.lineage_graph.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)

        return downstream

    def _apply_filters(self, assets: List[str],
                       filters: Dict) -> List[Dict]:
        """Apply filters to asset list."""
        result = []
        for asset_id in assets:
            metadata = self.asset_metadata.get(asset_id, {})
            if self._matches_filters(metadata, filters):
                result.append({"id": asset_id, **metadata})
        return result

    def _matches_filters(self, metadata: Dict, filters: Dict) -> bool:
        """Check if metadata matches filters."""
        for key, value in filters.items():
            if key == "tags" and isinstance(value, list):
                asset_tags = metadata.get("tags", [])
                if not any(tag in asset_tags for tag in value):
                    return False
            elif key in metadata and metadata[key] != value:
                return False
        return True

    def _get_applicable_policies(self, assets: List[Dict]) -> List[Dict]:
        """Get governance policies for assets."""
        policies = []
        for asset in assets:
            asset_id = asset.get("id")
            if asset_id in self.governance_policies:
                policies.extend(self.governance_policies[asset_id])
        return policies
