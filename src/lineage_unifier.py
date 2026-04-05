"""Universal lineage connector and unifier for Tracepipe AI."""
import json
from typing import Dict, List, Any, Optional
from datetime import datetime


class LineageNode:
    """Represents a node in the lineage graph."""
    def __init__(self, node_id: str, node_type: str, system: str, metadata: Optional[Dict] = None):
        self.node_id = node_id
        self.node_type = node_type
        self.system = system
        self.metadata = metadata or {}
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "system": self.system,
            "metadata": self.metadata,
            "timestamp": self.timestamp
        }


class LineageEdge:
    """Represents an edge in the lineage graph."""
    def __init__(self, source_id: str, target_id: str, edge_type: str = "derives_from"):
        self.source_id = source_id
        self.target_id = target_id
        self.edge_type = edge_type
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type,
            "timestamp": self.timestamp
        }


class LineageUnifier:
    """Unifies lineage from multiple systems."""
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.node_id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)

    def get_unified_lineage(self) -> Dict[str, Any]:
        return {
            "nodes": [node.to_dict() for node in self.nodes.values()],
            "edges": [edge.to_dict() for edge in self.edges]
        }

    def merge_lineage(self, external_lineage: Dict[str, Any]) -> None:
        for node_data in external_lineage.get("nodes", []):
            node = LineageNode(
                node_data["node_id"],
                node_data["node_type"],
                node_data["system"],
                node_data.get("metadata")
            )
            self.add_node(node)
        for edge_data in external_lineage.get("edges", []):
            edge = LineageEdge(
                edge_data["source_id"],
                edge_data["target_id"],
                edge_data.get("edge_type", "derives_from")
            )
            self.add_edge(edge)
