"""Visualize operational lineage graphs."""

from typing import Dict, List, Any
import json


class LineageVisualizer:
    """Visualizes operational lineage as JSON or text."""

    def __init__(self, lineage_graph):
        self.graph = lineage_graph

    def to_json(self) -> str:
        """Export graph to JSON format."""
        nodes_list = []
        for node_id, node in self.graph.nodes.items():
            nodes_list.append({
                'id': node_id,
                'type': node.node_type,
                'metadata': node.metadata
            })

        output = {
            'nodes': nodes_list,
            'edges': self.graph.edges
        }
        return json.dumps(output, indent=2)

    def to_text(self) -> str:
        """Generate a text-based visualization."""
        lines = ["Operational Lineage Graph", "=" * 40, ""]

        lines.append(f"Nodes: {len(self.graph.nodes)}")
        lines.append(f"Edges: {len(self.graph.edges)}")
        lines.append("")

        for node_type in ['notebook', 'job', 'dlt', 'dbt']:
            nodes = self.graph.get_nodes_by_type(node_type)
            if nodes:
                lines.append(f"{node_type.upper()}: {len(nodes)}")

        lines.append("")
        lines.append("Sample edges:")
        for edge in self.graph.edges[:10]:
            lines.append(
                f"  {edge['source']} --[{edge['type']}]--> {edge['target']}")

        return "\n".join(lines)

    def get_lineage_for_table(self, table_name: str) -> Dict[str, Any]:
        """Get upstream and downstream lineage for a specific table."""
        if table_name not in self.graph.nodes:
            return {'table': table_name, 'upstream': [], 'downstream': []}

        upstream = self.graph.get_upstream(table_name)
        downstream = self.graph.get_downstream(table_name)

        return {
            'table': table_name,
            'upstream': upstream,
            'downstream': downstream
        }

    def generate_impact_report(self, asset_id: str) -> Dict[str, Any]:
        """Generate an impact report for a code asset."""
        if asset_id not in self.graph.nodes:
            return {'asset': asset_id, 'impact': []}

        downstream = self.graph.get_downstream(asset_id)
        return {
            'asset': asset_id,
            'asset_type': self.graph.nodes[asset_id].node_type,
            'direct_impact': downstream,
            'impact_count': len(downstream)
        }
