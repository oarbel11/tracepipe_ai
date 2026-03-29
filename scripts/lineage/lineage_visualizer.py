from typing import Dict, List
import json


class LineageVisualizer:
    def __init__(self, extractor, analyzer):
        self.extractor = extractor
        self.analyzer = analyzer

    def generate_lineage_graph(self, table: str, column: str) -> Dict:
        """Generate lineage graph structure."""
        lineage = self.extractor.get_column_lineage(table, column)
        
        nodes = [{
            "id": f"{table}.{column}",
            "label": column,
            "type": "column",
            "table": table
        }]
        
        edges = []
        for upstream in lineage.get("upstream", []):
            nodes.append({
                "id": upstream,
                "label": upstream.split(".")[-1],
                "type": "column"
            })
            edges.append({
                "source": upstream,
                "target": f"{table}.{column}"
            })
        
        return {"nodes": nodes, "edges": edges}

    def generate_impact_graph(self, table: str, column: str) -> Dict:
        """Generate impact analysis graph."""
        impact = self.analyzer.analyze_column_change(table, column)
        
        nodes = [{
            "id": f"{table}.{column}",
            "label": column,
            "type": "source",
            "risk": impact["risk_level"]
        }]
        
        edges = []
        for affected_table in impact["affected_tables"]:
            nodes.append({
                "id": affected_table,
                "label": affected_table,
                "type": "affected_table"
            })
            edges.append({
                "source": f"{table}.{column}",
                "target": affected_table
            })
        
        return {"nodes": nodes, "edges": edges, "risk": impact["risk_level"]}

    def export_to_json(self, graph: Dict, output_path: str):
        """Export graph to JSON file."""
        with open(output_path, "w") as f:
            json.dump(graph, f, indent=2)

    def get_interactive_html(self, table: str, column: str) -> str:
        """Generate interactive HTML visualization."""
        graph = self.generate_lineage_graph(table, column)
        
        html = "<html><body><h1>Column Lineage</h1>"
        html += f"<h2>{table}.{column}</h2>"
        html += f"<pre>{json.dumps(graph, indent=2)}</pre>"
        html += "</body></html>"
        
        return html
