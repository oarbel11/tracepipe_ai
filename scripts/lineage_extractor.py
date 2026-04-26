from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from scripts.lineage_graph import LineageGraph
from scripts.external_lineage import ExternalLineageIntegrator
from scripts.impact_analyzer import ImpactAnalyzer

@dataclass
class ColumnNode:
    catalog: str
    schema: str
    table: str
    column: str

    def __str__(self):
        return f"{self.catalog}.{self.schema}.{self.table}.{self.column}"

class LineageExtractor:
    def __init__(self, workspace_url: Optional[str] = None, 
                 token: Optional[str] = None):
        self.workspace_url = workspace_url
        self.token = token
        self.graph = LineageGraph()
        self.integrator = ExternalLineageIntegrator(self.graph)
        self.analyzer = ImpactAnalyzer(self.graph)

    def extract_lineage(self, table_name: str) -> Dict[str, Any]:
        table_id = f"table:{table_name}"
        self.graph.add_node(table_id, 'table', table_name, {})
        return {
            'table': table_name,
            'upstream': [],
            'downstream': []
        }

    def get_column_lineage(self, column: ColumnNode) -> List[ColumnNode]:
        return []

    def add_external_source(self, source_id: str, source_type: str,
                           config: Dict[str, Any]):
        self.integrator.add_external_source(source_id, source_type, config)

    def integrate_etl_lineage(self, etl_tool: str, 
                             lineage_data: List[Dict]):
        self.integrator.integrate_etl_lineage(etl_tool, lineage_data)

    def integrate_bi_lineage(self, bi_tool: str, lineage_data: List[Dict]):
        self.integrator.integrate_bi_lineage(bi_tool, lineage_data)

    def integrate_file_lineage(self, file_path: str, table_id: str, 
                              operation: str):
        self.integrator.integrate_file_lineage(file_path, table_id, 
                                              operation)

    def analyze_impact(self, node_id: str) -> Dict:
        return self.analyzer.analyze_downstream_impact(node_id)

    def analyze_dependencies(self, node_id: str) -> Dict:
        return self.analyzer.analyze_upstream_dependencies(node_id)

    def handle_table_rename(self, old_name: str, new_name: str):
        old_id = f"table:{old_name}"
        new_id = f"table:{new_name}"
        self.analyzer.handle_table_rename(old_id, new_id)
