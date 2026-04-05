import json
import os
from typing import Dict, List
from scripts.transformation_tracer import TransformationTracer

class PeerReviewSystem:
    def __init__(self):
        self.transformation_tracer = TransformationTracer()
        self.reviews = []

    def analyze_code(self, code: str, code_type: str = "sql") -> Dict:
        if code_type.lower() == "sql":
            lineages = self.transformation_tracer.parse_sql(code)
        elif code_type.lower() == "python":
            lineages = self.transformation_tracer.parse_python(code)
        else:
            lineages = []
        
        self.transformation_tracer.build_graph(lineages)
        
        return {
            "lineages": [
                {
                    "source": l.source_column,
                    "target": l.target_column,
                    "transformation": l.transformation
                }
                for l in lineages
            ],
            "graph_nodes": list(self.transformation_tracer.graph.nodes()),
            "graph_edges": list(self.transformation_tracer.graph.edges())
        }

    def get_column_lineage(self, column_name: str) -> List[Dict]:
        lineage_path = []
        for lineage in self.transformation_tracer.column_lineages:
            if lineage.target_column == column_name:
                lineage_path.append({
                    "source": lineage.source_column,
                    "target": lineage.target_column,
                    "transformation": lineage.transformation
                })
        return lineage_path

    def export_lineage(self, output_path: str):
        data = {
            "lineages": [
                {
                    "source": l.source_column,
                    "target": l.target_column,
                    "transformation": l.transformation,
                    "source_table": l.source_table,
                    "target_table": l.target_table
                }
                for l in self.transformation_tracer.column_lineages
            ]
        }
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
