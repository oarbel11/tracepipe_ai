"""Lineage graph builder for cross-platform data flow visualization."""

from typing import Dict, List, Set, Optional


class LineageGraph:
    """Builds and manages lineage graphs across platforms."""

    def __init__(self):
        self._nodes: Dict[str, Dict] = {}
        self._edges: List[tuple] = []

    def add_node(self, node_id: str, node_type: str, metadata: Dict) -> None:
        """Add a node to the lineage graph."""
        self._nodes[node_id] = {
            "type": node_type,
            "metadata": metadata
        }

    def add_edge(self, source: str, target: str, edge_type: str = "flow") -> None:
        """Add an edge between two nodes."""
        self._edges.append((source, target, edge_type))

    def get_upstream(self, node_id: str) -> List[str]:
        """Get all upstream nodes for a given node."""
        return [src for src, tgt, _ in self._edges if tgt == node_id]

    def get_downstream(self, node_id: str) -> List[str]:
        """Get all downstream nodes for a given node."""
        return [tgt for src, tgt, _ in self._edges if src == node_id]

    def get_all_nodes(self) -> List[str]:
        """Get all node IDs in the graph."""
        return list(self._nodes.keys())

    def get_node_info(self, node_id: str) -> Optional[Dict]:
        """Get information about a specific node."""
        return self._nodes.get(node_id)

    def build_lineage_path(self, start: str, end: str) -> List[str]:
        """Find path between two nodes using BFS."""
        if start not in self._nodes or end not in self._nodes:
            return []
        
        queue = [[start]]
        visited: Set[str] = {start}
        
        while queue:
            path = queue.pop(0)
            node = path[-1]
            
            if node == end:
                return path
            
            for next_node in self.get_downstream(node):
                if next_node not in visited:
                    visited.add(next_node)
                    queue.append(path + [next_node])
        
        return []
