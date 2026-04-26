from typing import Dict, List, Set, Optional
from datetime import datetime

class LineageNode:
    def __init__(self, node_id: str, node_type: str, name: str, metadata: Optional[Dict] = None):
        self.node_id = node_id
        self.node_type = node_type
        self.name = name
        self.metadata = metadata or {}
        self.timestamp = datetime.now()

class LineageEdge:
    def __init__(self, source_id: str, target_id: str, edge_type: str, metadata: Optional[Dict] = None):
        self.source_id = source_id
        self.target_id = target_id
        self.edge_type = edge_type
        self.metadata = metadata or {}
        self.timestamp = datetime.now()

class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.adjacency: Dict[str, Set[str]] = {}
        self.reverse_adjacency: Dict[str, Set[str]] = {}

    def add_node(self, node_id: str, node_type: str, name: str, metadata: Optional[Dict] = None):
        node = LineageNode(node_id, node_type, name, metadata)
        self.nodes[node_id] = node
        if node_id not in self.adjacency:
            self.adjacency[node_id] = set()
        if node_id not in self.reverse_adjacency:
            self.reverse_adjacency[node_id] = set()

    def add_edge(self, source_id: str, target_id: str, edge_type: str, metadata: Optional[Dict] = None):
        edge = LineageEdge(source_id, target_id, edge_type, metadata)
        self.edges.append(edge)
        self.adjacency[source_id].add(target_id)
        self.reverse_adjacency[target_id].add(source_id)

    def get_downstream(self, node_id: str, depth: Optional[int] = None) -> List[str]:
        result = []
        visited = set()
        self._traverse_downstream(node_id, depth or float('inf'), 0, result, visited)
        return result

    def _traverse_downstream(self, node_id: str, max_depth: int, current_depth: int, result: List[str], visited: Set[str]):
        if current_depth >= max_depth or node_id in visited:
            return
        visited.add(node_id)
        if node_id in self.adjacency:
            for downstream_id in self.adjacency[node_id]:
                result.append(downstream_id)
                self._traverse_downstream(downstream_id, max_depth, current_depth + 1, result, visited)

    def get_upstream(self, node_id: str, depth: Optional[int] = None) -> List[str]:
        result = []
        visited = set()
        self._traverse_upstream(node_id, depth or float('inf'), 0, result, visited)
        return result

    def _traverse_upstream(self, node_id: str, max_depth: int, current_depth: int, result: List[str], visited: Set[str]):
        if current_depth >= max_depth or node_id in visited:
            return
        visited.add(node_id)
        if node_id in self.reverse_adjacency:
            for upstream_id in self.reverse_adjacency[node_id]:
                result.append(upstream_id)
                self._traverse_upstream(upstream_id, max_depth, current_depth + 1, result, visited)

class CrossSystemLineage:
    def __init__(self):
        self.graph = LineageGraph()
        self.table_renames: Dict[str, str] = {}

    def add_table(self, table_id: str, table_name: str, metadata: Optional[Dict] = None):
        self.graph.add_node(table_id, 'table', table_name, metadata or {})
