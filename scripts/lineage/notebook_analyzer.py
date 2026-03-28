from typing import Dict, List
import json
from .spark_parser import SparkColumnParser
from .lineage_graph import ColumnLineageGraph


class NotebookLineageAnalyzer:
    def __init__(self):
        self.parser = SparkColumnParser()
        self.graph = ColumnLineageGraph()
        self.notebook_metadata = {}

    def analyze_notebook(self, cells: List[Dict]) -> Dict:
        results = {"cells_analyzed": 0, "operations": 0, "udfs": 0}
        
        for idx, cell in enumerate(cells):
            if cell.get("cell_type") == "code":
                code = cell.get("source", "")
                if isinstance(code, list):
                    code = "".join(code)
                
                parsed = self.parser.parse_notebook_cell(code, idx)
                if "error" not in parsed:
                    self._process_parsed_cell(parsed)
                    results["cells_analyzed"] += 1
                    results["operations"] += len(parsed.get("operations", []))
                    results["udfs"] += len(parsed.get("udfs", []))
        
        return results

    def _process_parsed_cell(self, parsed: Dict):
        for op in parsed.get("operations", []):
            if op["type"] == "withColumn":
                cols = op.get("columns", [])
                if len(cols) >= 2:
                    target = cols[0].get("name")
                    sources = [c.get("name") for c in cols[1:]
                              if c.get("type") == "source"]
                    self.graph.add_transformation(
                        sources, target, "withColumn", parsed["cell_id"]
                    )
            elif op["type"] == "select":
                for col in op.get("columns", []):
                    if col.get("type") == "derived":
                        deps = col.get("deps", [])
                        flat_deps = [d[0].get("name") for d in deps if d]
                        self.graph.add_transformation(
                            flat_deps, col.get("name"), "select", parsed["cell_id"]
                        )

    def analyze_notebook_file(self, file_path: str) -> Dict:
        with open(file_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
        
        cells = notebook.get("cells", [])
        stats = self.analyze_notebook(cells)
        stats["notebook"] = file_path
        return stats

    def get_column_lineage(self, column: str) -> Dict:
        return self.graph.get_upstream_lineage(column)

    def get_impact_analysis(self, column: str) -> Dict:
        return self.graph.get_downstream_impact(column)

    def export_lineage_graph(self, output_path: str = None) -> str:
        viz = self.graph.to_json()
        if output_path:
            with open(output_path, 'w') as f:
                f.write(viz)
        return viz

    def register_table(self, table_name: str, columns: List[str]):
        self.graph.add_table(table_name, columns)
