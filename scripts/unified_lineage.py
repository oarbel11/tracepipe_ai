from typing import List, Dict, Set, Optional
from scripts.external_connectors import ExternalConnector, LineageNode, LineageEdge
from scripts.lineage_stitcher import LineageStitcher
import networkx as nx

class UnifiedLineageBuilder:
    def __init__(self):
        self.connectors: List[ExternalConnector] = []
        self.stitcher = LineageStitcher()
        self.graph: Optional[nx.DiGraph] = None
        self.nodes: List[LineageNode] = []
        self.edges: List[LineageEdge] = []

    def add_connector(self, connector: ExternalConnector):
        self.connectors.append(connector)

    def build_unified_graph(self) -> 'UnifiedLineageGraph':
        all_nodes, all_edges = [], []
        
        for connector in self.connectors:
            nodes, edges = connector.extract_lineage()
            all_nodes.extend(nodes)
            all_edges.extend(edges)
        
        stitched_edges = self.stitcher.stitch_lineage(all_nodes, all_edges)
        
        self.nodes = all_nodes
        self.edges = stitched_edges
        self.graph = self._build_networkx_graph(all_nodes, stitched_edges)
        
        return UnifiedLineageGraph(self.graph, all_nodes, stitched_edges)

    def _build_networkx_graph(self, nodes: List[LineageNode], edges: List[LineageEdge]) -> nx.DiGraph:
        G = nx.DiGraph()
        for node in nodes:
            node_id = f"{node.system}:{node.identifier}"
            G.add_node(node_id, **{'system': node.system, 'type': node.node_type, 'metadata': node.metadata})
        
        for edge in edges:
            source_id = f"{edge.source.system}:{edge.source.identifier}"
            target_id = f"{edge.target.system}:{edge.target.identifier}"
            G.add_edge(source_id, target_id, edge_type=edge.edge_type)
        
        return G

class UnifiedLineageGraph:
    def __init__(self, graph: nx.DiGraph, nodes: List[LineageNode], edges: List[LineageEdge]):
        self.graph = graph
        self.nodes = nodes
        self.edges = edges

    def get_downstream_impact(self, source_identifier: str) -> List[str]:
        matching_nodes = [n for n in self.graph.nodes() if source_identifier.lower() in n.lower()]
        if not matching_nodes:
            return []
        
        descendants = set()
        for node in matching_nodes:
            descendants.update(nx.descendants(self.graph, node))
        return list(descendants)

    def trace_path(self, source: str, target: str) -> List[str]:
        source_nodes = [n for n in self.graph.nodes() if source.lower() in n.lower()]
        target_nodes = [n for n in self.graph.nodes() if target.lower() in n.lower()]
        
        if not source_nodes or not target_nodes:
            return []
        
        try:
            path = nx.shortest_path(self.graph, source_nodes[0], target_nodes[0])
            return path
        except nx.NetworkXNoPath:
            return []

    def get_cross_system_edges(self) -> List[LineageEdge]:
        return [e for e in self.edges if e.edge_type == "cross_system_link"]
