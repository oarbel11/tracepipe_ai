import networkx as nx
from typing import Dict, List, Optional, Set
from scripts.peer_review.governance_policy import GovernancePolicy

class InteractiveImpactAnalyzer:
    """Analyzes impact of changes with filtering and policy overlay."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.policies: List[GovernancePolicy] = []

    def add_asset(self, asset_id: str, metadata: Dict):
        """Add asset to lineage graph."""
        self.graph.add_node(asset_id, **metadata)

    def add_dependency(self, source: str, target: str):
        """Add dependency between assets."""
        self.graph.add_edge(source, target)

    def add_policy(self, policy: GovernancePolicy):
        """Register a governance policy."""
        self.policies.append(policy)

    def analyze_impact(self, asset_id: str, filters: Optional[Dict] = None) -> Dict:
        """Analyze downstream impact with optional filters."""
        if asset_id not in self.graph:
            return {"error": f"Asset {asset_id} not found", "impacted_assets": []}

        descendants = nx.descendants(self.graph, asset_id)
        impacted = self._apply_filters(descendants, filters or {})
        
        result = {
            "source_asset": asset_id,
            "total_impacted": len(impacted),
            "impacted_assets": [],
            "policies": []
        }

        for node in impacted:
            asset_info = self._get_asset_info(node)
            asset_policies = self._get_applicable_policies(node)
            asset_info["applicable_policies"] = asset_policies
            result["impacted_assets"].append(asset_info)

        source_policies = self._get_applicable_policies(asset_id)
        result["policies"] = source_policies
        return result

    def _apply_filters(self, nodes: Set[str], filters: Dict) -> Set[str]:
        """Filter nodes based on criteria."""
        filtered = set()
        for node in nodes:
            if self._matches_filters(node, filters):
                filtered.add(node)
        return filtered

    def _matches_filters(self, node: str, filters: Dict) -> bool:
        """Check if node matches filter criteria."""
        metadata = self.graph.nodes[node]
        if "tags" in filters:
            node_tags = metadata.get("tags", [])
            if not any(tag in node_tags for tag in filters["tags"]):
                return False
        if "owner" in filters and metadata.get("owner") != filters["owner"]:
            return False
        if "quality_status" in filters and metadata.get("quality_status") != filters["quality_status"]:
            return False
        return True

    def _get_asset_info(self, node: str) -> Dict:
        """Get asset information."""
        metadata = dict(self.graph.nodes[node])
        metadata["asset_id"] = node
        return metadata

    def _get_applicable_policies(self, node: str) -> List[Dict]:
        """Get policies applicable to a node."""
        metadata = self.graph.nodes[node]
        tags = metadata.get("tags", [])
        return [p.to_dict() for p in self.policies if p.matches_asset(tags, node)]
