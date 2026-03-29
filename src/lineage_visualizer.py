from typing import Dict, Any, List

class LineageVisualizer:
    def generate_graph(self, lineage: Dict[str, Any]) -> Dict[str, Any]:
        nodes = []
        edges = []
        
        for col, info in lineage.get("columns", {}).items():
            nodes.append({"id": col, "type": "target"})
            
            for src_col in info.get("source_columns", []):
                if src_col not in [n["id"] for n in nodes]:
                    nodes.append({"id": src_col, "type": "source"})
                
                edges.append({
                    "from": src_col,
                    "to": col,
                    "transformation": info.get("transformation_type", "unknown")
                })
        
        return {"nodes": nodes, "edges": edges}
