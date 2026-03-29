"""Lineage visualization using ASCII and JSON output."""

import json
from typing import Dict, Any, List


class LineageVisualizer:
    """Visualize lineage graph."""

    def __init__(self, lineage_data: Dict[str, Any]):
        self.lineage_data = lineage_data

    def to_json(self) -> str:
        """Convert lineage to JSON string."""
        return json.dumps(self.lineage_data, indent=2)

    def to_ascii(self) -> str:
        """Generate ASCII representation of lineage."""
        nodes = self.lineage_data.get('nodes', [])
        edges = self.lineage_data.get('edges', [])
        
        lines = ["Databricks Lineage Graph", "=" * 50, ""]
        
        lines.append("Nodes:")
        for node in nodes:
            node_type = node.get('type', 'unknown')
            node_name = node.get('name', 'unknown')
            lines.append(f"  [{node_type}] {node_name}")
        
        lines.append("")
        lines.append("Edges:")
        for edge in edges:
            source = edge.get('source', 'unknown')
            target = edge.get('target', 'unknown')
            lines.append(f"  {source} -> {target}")
        
        return "\n".join(lines)

    def save_to_file(self, filepath: str, format: str = 'json') -> None:
        """Save lineage to file."""
        content = self.to_json() if format == 'json' else self.to_ascii()
        with open(filepath, 'w') as f:
            f.write(content)

    def get_statistics(self) -> Dict[str, int]:
        """Get lineage statistics."""
        nodes = self.lineage_data.get('nodes', [])
        edges = self.lineage_data.get('edges', [])
        
        node_types = {}
        for node in nodes:
            node_type = node.get('type', 'unknown')
            node_types[node_type] = node_types.get(node_type, 0) + 1
        
        return {
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            **node_types
        }
