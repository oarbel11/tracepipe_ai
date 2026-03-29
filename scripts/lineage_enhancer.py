import networkx as nx
from typing import Dict, List, Optional
from scripts.databricks_lineage_extractor import DatabricksLineageExtractor

class LineageEnhancer:
    def __init__(self):
        self.extractor = DatabricksLineageExtractor()
        self.lineage_graph = nx.DiGraph()

    def process_query(self, sql: str, query_plan: Optional[str] = None) -> nx.DiGraph:
        operations = self.extractor.extract_operations(sql)
        column_lineage = self.extractor.extract_column_lineage(sql)
        
        self._add_column_lineage(column_lineage)
        self._add_operation_nodes(operations)
        
        if query_plan:
            udfs = self.extractor.detect_python_udfs(query_plan)
            self._add_udf_nodes(udfs)
        
        return self.lineage_graph

    def _add_column_lineage(self, lineage: Dict[str, set]):
        for target, sources in lineage.items():
            self.lineage_graph.add_node(target, node_type='column')
            for source in sources:
                self.lineage_graph.add_node(source, node_type='column')
                self.lineage_graph.add_edge(source, target, edge_type='column_dependency')

    def _add_operation_nodes(self, operations: Dict[str, List[str]]):
        for op_type, items in operations.items():
            for item in items:
                node_id = f"{op_type}_{item}"
                self.lineage_graph.add_node(node_id, node_type=op_type, details=item)

    def _add_udf_nodes(self, udfs: List[Dict[str, str]]):
        for i, udf in enumerate(udfs):
            node_id = f"udf_{i}"
            self.lineage_graph.add_node(node_id, node_type='python_udf', **udf)

    def get_lineage_for_column(self, column: str) -> List[str]:
        if column not in self.lineage_graph:
            return []
        return list(nx.ancestors(self.lineage_graph, column))

    def get_impact_analysis(self, column: str) -> List[str]:
        if column not in self.lineage_graph:
            return []
        return list(nx.descendants(self.lineage_graph, column))

    def export_lineage(self) -> Dict:
        return {
            'nodes': [{'id': n, **attrs} for n, attrs in self.lineage_graph.nodes(data=True)],
            'edges': [{'source': u, 'target': v, **attrs} 
                     for u, v, attrs in self.lineage_graph.edges(data=True)]
        }
