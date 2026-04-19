from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import json

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False

class NodeType(Enum):
    TABLE = "table"
    VIEW = "view"
    COLUMN = "column"
    NOTEBOOK = "notebook"
    DASHBOARD = "dashboard"
    EXTERNAL = "external"

@dataclass
class LineageNode:
    id: str
    name: str
    node_type: NodeType
    workspace: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class LineageEdge:
    source_id: str
    target_id: str
    edge_type: str = "depends_on"
    metadata: Dict[str, Any] = field(default_factory=dict)

class UnifiedLineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.graph = nx.DiGraph() if HAS_NETWORKX else None
    
    def add_node(self, node: LineageNode):
        self.nodes[node.id] = node
        if self.graph is not None:
            self.graph.add_node(node.id, **node.__dict__)
    
    def add_edge(self, edge: LineageEdge):
        self.edges.append(edge)
        if self.graph is not None:
            self.graph.add_edge(edge.source_id, edge.target_id, **edge.__dict__)
    
    def get_upstream(self, node_id: str, depth: int = -1) -> Set[str]:
        if self.graph is not None:
            if node_id not in self.graph:
                return set()
            if depth == -1:
                return set(nx.ancestors(self.graph, node_id))
            upstream = set()
            self._get_ancestors_bfs(node_id, depth, upstream, is_upstream=True)
            return upstream
        return self._get_manual_upstream(node_id, depth)
    
    def get_downstream(self, node_id: str, depth: int = -1) -> Set[str]:
        if self.graph is not None:
            if node_id not in self.graph:
                return set()
            if depth == -1:
                return set(nx.descendants(self.graph, node_id))
            downstream = set()
            self._get_ancestors_bfs(node_id, depth, downstream, is_upstream=False)
            return downstream
        return self._get_manual_downstream(node_id, depth)
    
    def _get_ancestors_bfs(self, node_id: str, depth: int, result: Set[str], is_upstream: bool):
        if depth == 0:
            return
        neighbors = list(self.graph.predecessors(node_id)) if is_upstream else list(self.graph.successors(node_id))
        for neighbor in neighbors:
            if neighbor not in result:
                result.add(neighbor)
                self._get_ancestors_bfs(neighbor, depth - 1, result, is_upstream)
    
    def _get_manual_upstream(self, node_id: str, depth: int) -> Set[str]:
        result = set()
        for edge in self.edges:
            if edge.target_id == node_id:
                result.add(edge.source_id)
        return result
    
    def _get_manual_downstream(self, node_id: str, depth: int) -> Set[str]:
        result = set()
        for edge in self.edges:
            if edge.source_id == node_id:
                result.add(edge.target_id)
        return result
