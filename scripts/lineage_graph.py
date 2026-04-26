from enum import Enum
from typing import Dict, List, Set, Any, Optional


class NodeType(Enum):
    TABLE = 'table'
    VIEW = 'view'
    FILE = 'file'
    EXTERNAL_TABLE = 'external_table'
    ETL_JOB = 'etl_job'
    BI_REPORT = 'bi_report'


class LineageNode:
    def __init__(self, node_id: str, node_type: str, name: str,
                 metadata: Dict[str, Any]):
        self.node_id = node_id
        self.node_type = node_type
        self.name = name
        self.metadata = metadata or {}


class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: Dict[str, Set[str]] = {}
        self.reverse_edges: Dict[str, Set[str]] = {}

    def add_node(self, node_id: str, node_type: str, name: str,
                 metadata: Dict[str, Any]) -> None:
        node = LineageNode(node_id, node_type, name, metadata)
        self.nodes[node_id] = node
        if node_id not in self.edges:
            self.edges[node_id] = set()
        if node_id not in self.reverse_edges:
            self.reverse_edges[node_id] = set()

    def add_edge(self, source_id: str, target_id: str) -> None:
        if source_id not in self.edges:
            self.edges[source_id] = set()
        if target_id not in self.reverse_edges:
            self.reverse_edges[target_id] = set()
        self.edges[source_id].add(target_id)
        self.reverse_edges[target_id].add(source_id)

    def get_downstream(self, node_id: str) -> List[LineageNode]:
        result = []
        visited = set()
        self._traverse_downstream(node_id, visited, result)
        return result

    def _traverse_downstream(self, node_id: str, visited: Set[str],
                             result: List[LineageNode]) -> None:
        if node_id in visited or node_id not in self.edges:
            return
        visited.add(node_id)
        for target_id in self.edges.get(node_id, []):
            if target_id in self.nodes:
                result.append(self.nodes[target_id])
            self._traverse_downstream(target_id, visited, result)

    def get_upstream(self, node_id: str) -> List[LineageNode]:
        result = []
        visited = set()
        self._traverse_upstream(node_id, visited, result)
        return result

    def _traverse_upstream(self, node_id: str, visited: Set[str],
                           result: List[LineageNode]) -> None:
        if node_id in visited or node_id not in self.reverse_edges:
            return
        visited.add(node_id)
        for source_id in self.reverse_edges.get(node_id, []):
            if source_id in self.nodes:
                result.append(self.nodes[source_id])
            self._traverse_upstream(source_id, visited, result)
