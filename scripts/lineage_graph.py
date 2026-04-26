import networkx as nx
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
import json

@dataclass
class LineageNode:
    node_id: str
    node_type: str
    system: str = "unknown"
    metadata: Dict = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "system": self.system,
            "metadata": self.metadata,
            "tags": self.tags
        }

class LineageGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.rename_history: Dict[str, List[str]] = {}

    def add_node(self, node: LineageNode) -> None:
        self.graph.add_node(node.node_id, **node.to_dict())

    def add_edge(self, source: str, target: str, metadata: Dict = None) -> None:
        self.graph.add_edge(source, target, **(metadata or {}))

    def register_rename(self, old_name: str, new_name: str) -> None:
        if old_name not in self.rename_history:
            self.rename_history[old_name] = []
        self.rename_history[old_name].append(new_name)
        if self.graph.has_node(old_name):
            node_data = self.graph.nodes[old_name]
            self.graph.add_node(new_name, **node_data)
            for pred in list(self.graph.predecessors(old_name)):
                edge_data = self.graph.edges[pred, old_name]
                self.graph.add_edge(pred, new_name, **edge_data)
            for succ in list(self.graph.successors(old_name)):
                edge_data = self.graph.edges[old_name, succ]
                self.graph.add_edge(new_name, succ, **edge_data)

    def resolve_node(self, node_id: str) -> Optional[str]:
        if self.graph.has_node(node_id):
            return node_id
        for old, new_list in self.rename_history.items():
            if node_id == old:
                return new_list[-1] if new_list else None
        return None

    def get_upstream(self, node_id: str, max_depth: int = -1) -> Set[str]:
        resolved = self.resolve_node(node_id)
        if not resolved:
            return set()
        if max_depth == -1:
            return set(nx.ancestors(self.graph, resolved))
        upstream = set()
        for depth in range(1, max_depth + 1):
            for node in list(upstream) if upstream else [resolved]:
                upstream.update(self.graph.predecessors(node))
        return upstream

    def get_downstream(self, node_id: str, max_depth: int = -1) -> Set[str]:
        resolved = self.resolve_node(node_id)
        if not resolved:
            return set()
        if max_depth == -1:
            return set(nx.descendants(self.graph, resolved))
        downstream = set()
        for depth in range(1, max_depth + 1):
            for node in list(downstream) if downstream else [resolved]:
                downstream.update(self.graph.successors(node))
        return downstream
