from typing import Dict, Any
import json

class LineageVisualizer:
    def __init__(self):
        pass
    
    def generate_graph(self, lineage: Dict[str, Any]) -> Dict[str, Any]:
        nodes = []
        edges = []
        
        target = lineage["target_table"]
        
        for col_name, col_info in lineage["columns"].items():
            target_id = f"{target}.{col_name}"
            nodes.append({
                "id": target_id,
                "label": col_name,
                "type": "target"
            })
            
            for source_col in col_info.get("source_columns", []):
                source_id = f"source.{source_col}"
                
                if not any(n["id"] == source_id for n in nodes):
                    nodes.append({
                        "id": source_id,
                        "label": source_col,
                        "type": "source"
                    })
                
                edges.append({
                    "from": source_id,
                    "to": target_id,
                    "label": col_info.get("transformation_type", "unknown")
                })
        
        return {
            "nodes": nodes,
            "edges": edges
        }
    
    def export_to_json(self, graph: Dict[str, Any]) -> str:
        return json.dumps(graph, indent=2)
