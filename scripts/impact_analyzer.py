from typing import Dict, List, Set
from scripts.lineage_graph import LineageGraph, LineageNode

class ImpactAnalyzer:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def analyze_downstream_impact(self, node_id: str, depth: int = -1) -> Dict:
        downstream = self.graph.get_downstream(node_id, depth)
        impacted_nodes = []
        for node_id_downstream in downstream:
            node = self.graph.get_node(node_id_downstream)
            if node:
                impacted_nodes.append({
                    "id": node.id,
                    "type": node.node_type.value,
                    "system": node.system,
                    "metadata": node.metadata
                })
        return {
            "source": node_id,
            "impacted_count": len(impacted_nodes),
            "impacted_nodes": impacted_nodes
        }

    def analyze_upstream_dependencies(self, node_id: str, depth: int = -1) -> Dict:
        upstream = self.graph.get_upstream(node_id, depth)
        dependency_nodes = []
        for node_id_upstream in upstream:
            node = self.graph.get_node(node_id_upstream)
            if node:
                dependency_nodes.append({
                    "id": node.id,
                    "type": node.node_type.value,
                    "system": node.system,
                    "metadata": node.metadata
                })
        return {
            "target": node_id,
            "dependency_count": len(dependency_nodes),
            "dependency_nodes": dependency_nodes
        }

    def handle_table_rename(self, old_table_id: str, new_table_id: str):
        self.graph.add_alias(old_table_id, new_table_id)
        old_node = self.graph.get_node(old_table_id)
        if old_node:
            new_node = LineageNode(
                id=new_table_id,
                node_type=old_node.node_type,
                system=old_node.system,
                metadata={**old_node.metadata, "renamed_from": old_table_id}
            )
            self.graph.add_node(new_node)
            for target in self.graph.edges.get(old_table_id, set()):
                self.graph.add_edge(new_table_id, target)
            for source in self.graph.reverse_edges.get(old_table_id, set()):
                self.graph.add_edge(source, new_table_id)

    def get_cross_system_lineage(self, node_id: str) -> Dict:
        upstream = self.analyze_upstream_dependencies(node_id)
        downstream = self.analyze_downstream_impact(node_id)
        return {
            "node_id": node_id,
            "upstream": upstream,
            "downstream": downstream
        }
