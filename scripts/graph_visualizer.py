import json
from typing import Dict, Any


class GraphVisualizer:
    def __init__(self, graph_data: Dict[str, Any]):
        self.graph_data = graph_data

    def generate_ascii(self) -> str:
        lines = ["Impact Analysis Graph", "=" * 50, ""]
        nodes = self.graph_data.get('nodes', {})
        edges = self.graph_data.get('edges', [])
        lines.append(f"Nodes: {len(nodes)}")
        for node_id, node_info in nodes.items():
            node_type = node_info.get('type', 'unknown')
            node_name = node_info.get('name', node_id)
            lines.append(f"  [{node_type}] {node_name}")
        lines.append("")
        lines.append(f"Edges: {len(edges)}")
        for edge in edges:
            source = edge.get('source', '')
            target = edge.get('target', '')
            source_name = nodes.get(source, {}).get('name', source)
            target_name = nodes.get(target, {}).get('name', target)
            lines.append(f"  {source_name} -> {target_name}")
        return "\n".join(lines)

    def generate_json(self) -> str:
        return json.dumps(self.graph_data, indent=2)

    def generate_mermaid(self) -> str:
        lines = ["graph TD"]
        nodes = self.graph_data.get('nodes', {})
        edges = self.graph_data.get('edges', [])
        for node_id, node_info in nodes.items():
            node_name = node_info.get('name', node_id)
            node_type = node_info.get('type', 'unknown')
            safe_id = node_id.replace('-', '_').replace('.', '_')
            lines.append(f"    {safe_id}[{node_name}<br/>{node_type}]")
        for edge in edges:
            source = edge.get('source', '').replace('-', '_').replace('.', '_')
            target = edge.get('target', '').replace('-', '_').replace('.', '_')
            lines.append(f"    {source} --> {target}")
        return "\n".join(lines)
