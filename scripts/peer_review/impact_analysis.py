"""Impact analysis engine for Tracepipe AI."""
from typing import List, Dict, Set, Optional
from scripts.peer_review.lineage_graph import LineageGraph, LineageNode

class ImpactNode:
    def __init__(self, node_id: str, node_type: str, depth: int, metadata: dict):
        self.node_id = node_id
        self.node_type = node_type
        self.depth = depth
        self.metadata = metadata

class ImpactAnalysisEngine:
    def __init__(self, lineage_graph: LineageGraph):
        self.graph = lineage_graph

    def analyze_downstream_impact(self, node_id: str, filters: Dict = None) -> List[ImpactNode]:
        filters = filters or {}
        visited = set()
        result = []
        self._traverse_downstream(node_id, 0, visited, result, filters)
        return result

    def _traverse_downstream(self, node_id: str, depth: int, visited: Set, result: List, filters: Dict):
        if node_id in visited:
            return
        visited.add(node_id)
        node = self.graph.get_node(node_id)
        if not node:
            return
        if self._matches_filters(node, filters):
            result.append(ImpactNode(node_id, node.node_type, depth, node.metadata))
        for downstream_id in self.graph.get_downstream_nodes(node_id):
            self._traverse_downstream(downstream_id, depth + 1, visited, result, filters)

    def _matches_filters(self, node: LineageNode, filters: Dict) -> bool:
        if 'tags' in filters and filters['tags']:
            if not any(tag in node.tags for tag in filters['tags']):
                return False
        if 'owner' in filters and filters['owner']:
            if node.owner != filters['owner']:
                return False
        if 'quality_status' in filters and filters['quality_status']:
            if node.quality_status != filters['quality_status']:
                return False
        return True

    def get_blast_radius(self, node_id: str) -> int:
        impact_nodes = self.analyze_downstream_impact(node_id)
        return len(impact_nodes)
