"""Impact analysis engine for interactive dependency visualization."""
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass


@dataclass
class ImpactNode:
    """Node in impact analysis graph."""
    asset_id: str
    asset_type: str
    depth: int
    metadata: Dict[str, Any]


class ImpactAnalysisEngine:
    """Engine for computing downstream dependencies."""

    def __init__(self, lineage_graph: Dict[str, Any]):
        """Initialize with lineage graph."""
        self.lineage_graph = lineage_graph
        self.edges = lineage_graph.get("edges", [])
        self.nodes = {n["id"]: n for n in lineage_graph.get("nodes", [])}

    def compute_downstream_impact(
        self,
        asset_id: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[ImpactNode]:
        """Compute downstream dependencies with optional filters."""
        visited: Set[str] = set()
        result: List[ImpactNode] = []
        queue: List[tuple] = [(asset_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            node = self.nodes.get(current_id)
            if not node:
                continue

            if self._matches_filters(node, filters):
                result.append(ImpactNode(
                    asset_id=current_id,
                    asset_type=node.get("type", "unknown"),
                    depth=depth,
                    metadata=node.get("metadata", {})
                ))

            for edge in self.edges:
                if edge["source"] == current_id:
                    queue.append((edge["target"], depth + 1))

        return result

    def _matches_filters(self, node: Dict, filters: Optional[Dict]) -> bool:
        """Check if node matches filter criteria."""
        if not filters:
            return True
        metadata = node.get("metadata", {})
        for key, value in filters.items():
            if key == "tags" and value:
                node_tags = metadata.get("tags", [])
                if not any(tag in node_tags for tag in value):
                    return False
            elif key == "owner" and metadata.get("owner") != value:
                return False
            elif key == "quality_status" and metadata.get("quality_status") != value:
                return False
        return True
