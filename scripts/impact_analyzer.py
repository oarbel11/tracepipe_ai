from typing import Dict, List, Set, Optional
from scripts.lineage_graph import LineageGraph, LineageNode

class ImpactAnalyzer:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def analyze_downstream_impact(self, node_id: str, 
                                 max_depth: int = -1) -> Dict:
        downstream = self.graph.get_downstream(node_id, max_depth)
        impacted_nodes = []
        for node_id in downstream:
            node = self.graph.get_node(node_id)
            if node:
                impacted_nodes.append({
                    'id': node.id,
                    'type': node.type,
                    'name': node.name,
                    'metadata': node.metadata
                })
        return {
            'source': node_id,
            'impacted_count': len(impacted_nodes),
            'impacted_nodes': impacted_nodes
        }

    def analyze_upstream_dependencies(self, node_id: str, 
                                     max_depth: int = -1) -> Dict:
        upstream = self.graph.get_upstream(node_id, max_depth)
        dependency_nodes = []
        for node_id in upstream:
            node = self.graph.get_node(node_id)
            if node:
                dependency_nodes.append({
                    'id': node.id,
                    'type': node.type,
                    'name': node.name,
                    'metadata': node.metadata
                })
        return {
            'target': node_id,
            'dependency_count': len(dependency_nodes),
            'dependency_nodes': dependency_nodes
        }

    def handle_table_rename(self, old_id: str, new_id: str):
        old_node = self.graph.get_node(old_id)
        if not old_node:
            return
        self.graph.add_node(new_id, old_node.type, new_id, 
                          old_node.metadata)
        for upstream_id in self.graph.reverse_edges.get(old_id, set()):
            self.graph.add_edge(upstream_id, new_id)
        for downstream_id in self.graph.edges.get(old_id, set()):
            self.graph.add_edge(new_id, downstream_id)
        if old_id in self.graph.nodes:
            del self.graph.nodes[old_id]
        if old_id in self.graph.edges:
            del self.graph.edges[old_id]
        if old_id in self.graph.reverse_edges:
            del self.graph.reverse_edges[old_id]
