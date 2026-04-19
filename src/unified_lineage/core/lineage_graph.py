from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field

@dataclass
class LineageNode:
    id: str
    type: str
    metadata: Dict = field(default_factory=dict)

@dataclass
class LineageEdge:
    source: str
    target: str
    type: str
    metadata: Dict = field(default_factory=dict)

class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []

    def add_node(self, node: LineageNode):
        self.nodes[node.id] = node

    def add_edge(self, edge: LineageEdge):
        self.edges.append(edge)

    def get_upstream(self, node_id: str) -> Set[str]:
        upstream = set()
        visited = set()
        self._collect_upstream(node_id, upstream, visited)
        upstream.discard(node_id)
        return upstream

    def _collect_upstream(self, node_id: str, upstream: Set[str], visited: Set[str]):
        if node_id in visited:
            return
        visited.add(node_id)
        upstream.add(node_id)
        for edge in self.edges:
            if edge.target == node_id:
                self._collect_upstream(edge.source, upstream, visited)

    def get_downstream(self, node_id: str) -> Set[str]:
        downstream = set()
        visited = set()
        self._collect_downstream(node_id, downstream, visited)
        downstream.discard(node_id)
        return downstream

    def _collect_downstream(self, node_id: str, downstream: Set[str], visited: Set[str]):
        if node_id in visited:
            return
        visited.add(node_id)
        downstream.add(node_id)
        for edge in self.edges:
            if edge.source == node_id:
                self._collect_downstream(edge.target, downstream, visited)

    def merge(self, other_graph: 'LineageGraph'):
        for node in other_graph.nodes.values():
            if node.id not in self.nodes:
                self.add_node(node)
        for edge in other_graph.edges:
            self.add_edge(edge)

    def to_dict(self) -> Dict:
        return {
            'nodes': [{'id': n.id, 'type': n.type, 'metadata': n.metadata} 
                     for n in self.nodes.values()],
            'edges': [{'source': e.source, 'target': e.target, 
                      'type': e.type, 'metadata': e.metadata} 
                     for e in self.edges]
        }
