from typing import Dict, List
import json

class LineageVisualizer:
    def __init__(self):
        pass
    
    def generate_graph(self, lineage_data: Dict) -> Dict:
        nodes = []
        edges = []
        
        table = lineage_data.get('table')
        columns = lineage_data.get('columns', {})
        
        nodes.append({
            "id": table,
            "label": table,
            "type": "table"
        })
        
        for col_name, col_info in columns.items():
            col_id = f"{table}.{col_name}"
            nodes.append({
                "id": col_id,
                "label": col_name,
                "type": "column",
                "transformation": col_info.get('transformation_type')
            })
            
            edges.append({
                "from": table,
                "to": col_id,
                "type": "contains"
            })
            
            for source in col_info.get('source_columns', []):
                source_id = f"source.{source}"
                if not any(n['id'] == source_id for n in nodes):
                    nodes.append({
                        "id": source_id,
                        "label": source,
                        "type": "source_column"
                    })
                
                edges.append({
                    "from": source_id,
                    "to": col_id,
                    "type": "lineage",
                    "transformation": col_info.get('transformation_type')
                })
        
        return {"nodes": nodes, "edges": edges}
    
    def export_to_json(self, graph_data: Dict) -> str:
        return json.dumps(graph_data, indent=2)
