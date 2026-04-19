import networkx as nx
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class LineageNode:
    node_id: str
    node_type: str
    platform: str
    metadata: Dict = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "platform": self.platform,
            "metadata": self.metadata,
            "tags": self.tags,
            "timestamp": self.timestamp
        }

class UnifiedLineageGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes: Dict[str, LineageNode] = {}

    def add_node(self, node: LineageNode):
        self.nodes[node.node_id] = node
        self.graph.add_node(node.node_id, **node.to_dict())

    def add_edge(self, source_id: str, target_id: str, edge_type: str = "derives_from"):
        self.graph.add_edge(source_id, target_id, edge_type=edge_type)

    def get_upstream(self, node_id: str, max_depth: Optional[int] = None) -> List[str]:
        if node_id not in self.graph:
            return []
        if max_depth:
            return list(nx.ancestors(self.graph, node_id))[:max_depth]
        return list(nx.ancestors(self.graph, node_id))

    def get_downstream_impact(self, node_id: str) -> List[str]:
        if node_id not in self.graph:
            return []
        return list(nx.descendants(self.graph, node_id))

    def get_path(self, source_id: str, target_id: str) -> List[List[str]]:
        try:
            return list(nx.all_simple_paths(self.graph, source_id, target_id))
        except (nx.NodeNotFound, nx.NetworkXNoPath):
            return []

    def get_cross_platform_paths(self) -> List[Tuple[str, str, List[str]]]:
        paths = []
        for node_id in self.graph.nodes():
            node = self.nodes.get(node_id)
            if not node:
                continue
            for descendant in self.get_downstream_impact(node_id):
                desc_node = self.nodes.get(descendant)
                if desc_node and desc_node.platform != node.platform:
                    path = nx.shortest_path(self.graph, node_id, descendant)
                    paths.append((node.platform, desc_node.platform, path))
        return paths

    def to_dict(self) -> Dict:
        return {
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "edges": [{
                "source": u,
                "target": v,
                "type": d.get("edge_type", "derives_from")
            } for u, v, d in self.graph.edges(data=True)]
        }
