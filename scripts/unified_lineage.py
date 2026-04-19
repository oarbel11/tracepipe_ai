import networkx as nx
from typing import Dict, List, Any, Optional


class LineageNode:
    """Represents a node in the unified lineage graph"""
    def __init__(self, node_id: str, node_type: str, platform: str, metadata: Optional[Dict] = None):
        self.node_id = node_id
        self.node_type = node_type
        self.platform = platform
        self.metadata = metadata or {}

    def __repr__(self):
        return f"LineageNode({self.node_id}, {self.platform})"


class UnifiedLineageGraph:
    """Unified cross-platform lineage graph"""
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_node(self, node: LineageNode):
        """Add a node to the graph"""
        self.graph.add_node(node.node_id, node_type=node.node_type, platform=node.platform, metadata=node.metadata)

    def add_edge(self, source_id: str, target_id: str, edge_type: str = "transforms"):
        """Add an edge between nodes"""
        self.graph.add_edge(source_id, target_id, edge_type=edge_type)

    def get_upstream(self, node_id: str) -> List[str]:
        """Get all upstream dependencies"""
        if node_id not in self.graph:
            return []
        return list(self.graph.predecessors(node_id))

    def get_downstream(self, node_id: str) -> List[str]:
        """Get all downstream dependencies"""
        if node_id not in self.graph:
            return []
        return list(self.graph.successors(node_id))

    def get_impact_analysis(self, node_id: str) -> Dict[str, List[str]]:
        """Perform impact analysis for a node"""
        return {"upstream": self.get_upstream(node_id), "downstream": self.get_downstream(node_id)}

    def merge_lineage(self, external_graph: Dict[str, Any]):
        """Merge external lineage into unified graph"""
        for node in external_graph.get("nodes", []):
            node_obj = LineageNode(node["id"], node.get("type", "table"), node.get("platform", "external"), node.get("metadata"))
            self.add_node(node_obj)

        for edge in external_graph.get("edges", []):
            self.add_edge(edge["source"], edge["target"], edge.get("type", "transforms"))

    def export_graph(self) -> Dict[str, Any]:
        """Export graph as dictionary"""
        nodes = [{"id": n, **self.graph.nodes[n]} for n in self.graph.nodes()]
        edges = [{"source": u, "target": v, **self.graph.edges[u, v]} for u, v in self.graph.edges()]
        return {"nodes": nodes, "edges": edges}
