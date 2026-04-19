from typing import Dict, List
from ..core.lineage_graph import LineageGraph, LineageNode, LineageEdge

class UnityCatalogExtractor:
    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url
        self.token = token

    def extract_table_lineage(self, table_name: str) -> LineageGraph:
        graph = LineageGraph()
        table_node = LineageNode(
            id=table_name,
            type='table',
            metadata={'source': 'unity_catalog'}
        )
        graph.add_node(table_node)
        return graph

    def extract_column_lineage(self, table_name: str, column_name: str) -> LineageGraph:
        graph = LineageGraph()
        column_id = f"{table_name}.{column_name}"
        column_node = LineageNode(
            id=column_id,
            type='column',
            metadata={'table': table_name, 'column': column_name}
        )
        graph.add_node(column_node)
        return graph
