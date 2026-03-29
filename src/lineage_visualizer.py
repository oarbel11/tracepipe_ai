from typing import Dict, List
import json

class LineageVisualizer:
    def __init__(self):
        self.graph_data = {"nodes": [], "edges": []}
    
    def generate_graph(self, lineage: Dict) -> Dict:
        nodes = []
        edges = []
        node_ids = set()
        
        target_table = lineage["target_table"]
        
        for col_name, col_info in lineage["columns"].items():
            target_id = f"{target_table}.{col_name}"
            if target_id not in node_ids:
                nodes.append({
                    "id": target_id,
                    "label": col_name,
                    "type": "target",
                    "table": target_table
                })
                node_ids.add(target_id)
            
            for source_col in col_info["source_columns"]:
                source_id = f"source.{source_col}"
                if source_id not in node_ids:
                    nodes.append({
                        "id": source_id,
                        "label": source_col,
                        "type": "source"
                    })
                    node_ids.add(source_id)
                
                edges.append({
                    "from": source_id,
                    "to": target_id,
                    "transformation": col_info["transformation_type"]
                })
        
        return {"nodes": nodes, "edges": edges}
    
    def render_ascii(self, graph: Dict) -> str:
        lines = []
        lines.append("Column Lineage Graph:")
        lines.append("=" * 50)
        
        for edge in graph["edges"]:
            source_node = next(n for n in graph["nodes"] if n["id"] == edge["from"])
            target_node = next(n for n in graph["nodes"] if n["id"] == edge["to"])
            lines.append(f"{source_node['label']} --[{edge['transformation']}]--> {target_node['label']}")
        
        return "\n".join(lines)
    
    def export_json(self, graph: Dict) -> str:
        return json.dumps(graph, indent=2)
