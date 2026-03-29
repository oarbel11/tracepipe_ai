import json
from typing import Dict, List


class LineageVisualizer:
    def __init__(self):
        self.graph_data = {"nodes": [], "edges": []}

    def create_graph(self, lineage: Dict) -> Dict:
        """Create graph representation of lineage."""
        nodes = []
        edges = []
        
        target_table = lineage.get("target_table", "unknown")
        
        for col_name, col_info in lineage.get("columns", {}).items():
            target_node = f"{target_table}.{col_name}"
            nodes.append({
                "id": target_node,
                "label": col_name,
                "table": target_table,
                "type": "target"
            })
            
            for src_col in col_info.get("source_columns", []):
                nodes.append({
                    "id": src_col,
                    "label": src_col.split(".")[-1],
                    "table": src_col.split(".")[0],
                    "type": "source"
                })
                
                edges.append({
                    "from": src_col,
                    "to": target_node,
                    "transformation": col_info.get("transformation_type"),
                    "expression": col_info.get("expression")
                })
        
        unique_nodes = {node["id"]: node for node in nodes}
        
        return {
            "nodes": list(unique_nodes.values()),
            "edges": edges
        }

    def export_to_json(self, graph: Dict) -> str:
        """Export graph to JSON format."""
        return json.dumps(graph, indent=2)

    def generate_impact_visualization(self, impact: Dict) -> Dict:
        """Generate visualization for impact analysis."""
        nodes = [{"id": impact["source"], "type": "origin"}]
        edges = []
        
        for item in impact.get("transformation_chain", []):
            nodes.append({"id": item["to"], "type": "affected"})
            edges.append({
                "from": item["from"],
                "to": item["to"],
                "transformation": item["transformation"]
            })
        
        return {"nodes": nodes, "edges": edges}
