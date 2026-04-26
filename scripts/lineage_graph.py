from enum import Enum
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field


class NodeType(Enum):
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    NOTEBOOK = "notebook"
    PIPELINE = "pipeline"
    BI_REPORT = "bi_report"


@dataclass
class LineageNode:
    id: str
    name: str
    node_type: NodeType
    metadata: Dict = field(default_factory=dict)


class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: Dict[str, Set[str]] = {}
        self.reverse_edges: Dict[str, Set[str]] = {}

    def add_node(self, node: LineageNode):
        self.nodes[node.id] = node
        if node.id not in self.edges:
            self.edges[node.id] = set()
        if node.id not in self.reverse_edges:
            self.reverse_edges[node.id] = set()

    def add_edge(self, from_id: str, to_id: str):
        if from_id not in self.edges:
            self.edges[from_id] = set()
        if to_id not in self.reverse_edges:
            self.reverse_edges[to_id] = set()
        self.edges[from_id].add(to_id)
        self.reverse_edges[to_id].add(from_id)

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        return self.nodes.get(node_id)

    def get_downstream(self, node_id: str) -> List[str]:
        visited = set()
        result = []
        self._dfs_downstream(node_id, visited, result)
        return result

    def _dfs_downstream(self, node_id: str, visited: Set[str], result: List[str]):
        if node_id in visited:
            return
        visited.add(node_id)
        for child in self.edges.get(node_id, []):
            result.append(child)
            self._dfs_downstream(child, visited, result)

    def get_upstream(self, node_id: str) -> List[str]:
        visited = set()
        result = []
        self._dfs_upstream(node_id, visited, result)
        return result

    def _dfs_upstream(self, node_id: str, visited: Set[str], result: List[str]):
        if node_id in visited:
            return
        visited.add(node_id)
        for parent in self.reverse_edges.get(node_id, []):
            result.append(parent)
            self._dfs_upstream(parent, visited, result)
