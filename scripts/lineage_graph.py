from typing import Dict, Set, List, Optional
from dataclasses import dataclass
from enum import Enum

class NodeType(Enum):
    TABLE = "table"
    FILE = "file"
    BI_REPORT = "bi_report"
    ETL_JOB = "etl_job"

@dataclass
class LineageNode:
    id: str
    node_type: NodeType
    system: str
    metadata: Dict

class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: Dict[str, Set[str]] = {}
        self.reverse_edges: Dict[str, Set[str]] = {}
        self.aliases: Dict[str, str] = {}

    def add_node(self, node: LineageNode):
        self.nodes[node.id] = node
        if node.id not in self.edges:
            self.edges[node.id] = set()
        if node.id not in self.reverse_edges:
            self.reverse_edges[node.id] = set()

    def add_edge(self, source_id: str, target_id: str, metadata: Dict = None):
        if source_id not in self.edges:
            self.edges[source_id] = set()
        if target_id not in self.reverse_edges:
            self.reverse_edges[target_id] = set()
        self.edges[source_id].add(target_id)
        self.reverse_edges[target_id].add(source_id)

    def add_alias(self, old_id: str, new_id: str):
        self.aliases[old_id] = new_id

    def resolve_id(self, node_id: str) -> str:
        return self.aliases.get(node_id, node_id)

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        resolved_id = self.resolve_id(node_id)
        return self.nodes.get(resolved_id)

    def get_downstream(self, node_id: str, depth: int = -1) -> Set[str]:
        resolved_id = self.resolve_id(node_id)
        result = set()
        visited = set()
        self._traverse_downstream(resolved_id, depth, 0, result, visited)
        return result

    def _traverse_downstream(self, node_id: str, max_depth: int, current_depth: int, result: Set[str], visited: Set[str]):
        if node_id in visited or (max_depth >= 0 and current_depth > max_depth):
            return
        visited.add(node_id)
        for target in self.edges.get(node_id, set()):
            result.add(target)
            self._traverse_downstream(target, max_depth, current_depth + 1, result, visited)

    def get_upstream(self, node_id: str, depth: int = -1) -> Set[str]:
        resolved_id = self.resolve_id(node_id)
        result = set()
        visited = set()
        self._traverse_upstream(resolved_id, depth, 0, result, visited)
        return result

    def _traverse_upstream(self, node_id: str, max_depth: int, current_depth: int, result: Set[str], visited: Set[str]):
        if node_id in visited or (max_depth >= 0 and current_depth > max_depth):
            return
        visited.add(node_id)
        for source in self.reverse_edges.get(node_id, set()):
            result.add(source)
            self._traverse_upstream(source, max_depth, current_depth + 1, result, visited)
