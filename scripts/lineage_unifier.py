"""Lineage unification engine to merge external and Databricks lineage."""
import json
from typing import Dict, List, Any
from pathlib import Path
import networkx as nx
from connectors import ConnectorRegistry


class LineageUnifier:
    """Unifies lineage from multiple sources into a single graph."""

    def __init__(self):
        self.graph = nx.DiGraph()

    def add_lineage_from_connector(self, connector_name: str, config: Dict[str, Any]):
        """Add lineage from a specific connector."""
        connector = ConnectorRegistry.get_connector(connector_name, config)
        lineage_data = connector.extract_lineage()

        for node in lineage_data.get("nodes", []):
            self.graph.add_node(node["id"], **node)

        for edge in lineage_data.get("edges", []):
            self.graph.add_edge(edge["source_id"], edge["target_id"], edge_type=edge["edge_type"])

    def add_databricks_lineage(self, lineage_data: Dict[str, Any]):
        """Add Databricks Unity Catalog lineage."""
        for node in lineage_data.get("nodes", []):
            node_id = f"databricks:{node['id']}"
            self.graph.add_node(node_id, **node, system="databricks")

        for edge in lineage_data.get("edges", []):
            source_id = f"databricks:{edge['source_id']}"
            target_id = f"databricks:{edge['target_id']}"
            self.graph.add_edge(source_id, target_id, edge_type=edge.get("edge_type", "depends_on"))

    def link_systems(self, mappings: List[Dict[str, str]]):
        """Create edges between different systems based on mappings."""
        for mapping in mappings:
            source = mapping.get("source")
            target = mapping.get("target")
            if source and target and self.graph.has_node(source) and self.graph.has_node(target):
                self.graph.add_edge(source, target, edge_type="cross_system")

    def get_unified_lineage(self) -> Dict[str, Any]:
        """Export unified lineage graph."""
        nodes = [dict(id=n, **self.graph.nodes[n]) for n in self.graph.nodes()]
        edges = [{"source_id": u, "target_id": v, **self.graph.edges[u, v]} for u, v in self.graph.edges()]
        return {"nodes": nodes, "edges": edges}

    def export_to_file(self, output_path: str):
        """Export unified lineage to JSON file."""
        unified = self.get_unified_lineage()
        with open(output_path, "w") as f:
            json.dump(unified, f, indent=2)

    def get_downstream_impact(self, node_id: str) -> List[str]:
        """Get all downstream nodes affected by a change."""
        if not self.graph.has_node(node_id):
            return []
        return list(nx.descendants(self.graph, node_id))
