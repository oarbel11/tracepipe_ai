from typing import Dict, Set, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class LineageNode:
    id: str
    type: str
    name: str
    metadata: Dict[str, Any]

class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: Dict[str, Set[str]] = {}
        self.reverse_edges: Dict[str, Set[str]] = {}

    def add_node(self, node_id: str, node_type: str, name: str, 
                 metadata: Optional[Dict] = None):
        self.nodes[node_id] = LineageNode(
            id=node_id, type=node_type, name=name, 
            metadata=metadata or {}
        )
        if node_id not in self.edges:
            self.edges[node_id] = set()
        if node_id not in self.reverse_edges:
            self.reverse_edges[node_id] = set()

    def add_edge(self, source_id: str, target_id: str):
        if source_id not in self.edges:
            self.edges[source_id] = set()
        if target_id not in self.reverse_edges:
            self.reverse_edges[target_id] = set()
        self.edges[source_id].add(target_id)
        self.reverse_edges[target_id].add(source_id)

    def get_downstream(self, node_id: str, max_depth: int = -1) -> Set[str]:
        visited = set()
        queue = [(node_id, 0)]
        while queue:
            current, depth = queue.pop(0)
            if current in visited:
                continue
            if max_depth >= 0 and depth > max_depth:
                continue
            visited.add(current)
            for neighbor in self.edges.get(current, set()):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))
        visited.discard(node_id)
        return visited

    def get_upstream(self, node_id: str, max_depth: int = -1) -> Set[str]:
        visited = set()
        queue = [(node_id, 0)]
        while queue:
            current, depth = queue.pop(0)
            if current in visited:
                continue
            if max_depth >= 0 and depth > max_depth:
                continue
            visited.add(current)
            for neighbor in self.reverse_edges.get(current, set()):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))
        visited.discard(node_id)
        return visited

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        return self.nodes.get(node_id)
