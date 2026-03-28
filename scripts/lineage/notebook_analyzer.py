"""Analyze Databricks notebooks for column-level lineage."""

from typing import Dict, List
from .spark_parser import SparkColumnParser
from .lineage_graph import ColumnLineageGraph


class NotebookLineageAnalyzer:
    """Analyzes notebooks to extract column lineage."""

    def __init__(self):
        self.parser = SparkColumnParser()
        self.graph = ColumnLineageGraph()

    def analyze_notebook(self, notebook_path: str, cells: List[Dict]) -> Dict:
        """Analyze notebook cells to extract lineage."""
        all_mappings = []
        
        for cell in cells:
            if cell.get("cell_type") == "code":
                code = cell.get("source", "")
                language = cell.get("language", "python")
                
                if language == "python":
                    mappings = self.parser.parse_python_code(code)
                elif language == "scala":
                    mappings = self.parser.parse_scala_code(code)
                else:
                    mappings = []
                
                all_mappings.extend(mappings)
        
        return {
            "notebook_path": notebook_path,
            "total_transformations": len(all_mappings),
            "transformations": all_mappings
        }

    def build_lineage_graph(self, mappings: List[Dict], 
                           source_table: str, target_table: str) -> ColumnLineageGraph:
        """Build a lineage graph from transformation mappings."""
        graph = ColumnLineageGraph()
        graph.build_from_mappings(mappings, source_table, target_table)
        return graph

    def get_column_impact(self, column_id: str) -> Dict:
        """Analyze impact of changes to a specific column."""
        upstream = self.graph.get_upstream_columns(column_id)
        downstream = self.graph.get_downstream_columns(column_id)
        
        return {
            "column": column_id,
            "upstream_dependencies": upstream,
            "downstream_impacts": downstream,
            "total_upstream": len(upstream),
            "total_downstream": len(downstream)
        }

    def visualize_lineage(self, output_format: str = "json") -> str:
        """Generate lineage visualization."""
        if output_format == "json":
            return self.graph.to_json()
        return ""

    def analyze_notebook_file(self, file_path: str) -> Dict:
        """Analyze a notebook file directly."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            cells = [{"cell_type": "code", "source": content, "language": "python"}]
            return self.analyze_notebook(file_path, cells)
        except Exception as e:
            return {
                "notebook_path": file_path,
                "error": str(e),
                "total_transformations": 0,
                "transformations": []
            }
