from databricks.sdk import WorkspaceClient
from typing import Dict, List
from scripts.lineage_graph import LineageGraph, LineageNode, NodeType
from scripts.external_lineage_integrator import ExternalLineageIntegrator
from scripts.impact_analyzer import ImpactAnalyzer

class LineageExtractor:
    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client
        self.graph = LineageGraph()
        self.integrator = ExternalLineageIntegrator(self.graph)
        self.analyzer = ImpactAnalyzer(self.graph)

    def extract_table_lineage(self, catalog: str, schema: str, table: str) -> Dict:
        table_id = f"{catalog}.{schema}.{table}"
        table_node = LineageNode(
            id=table_id,
            node_type=NodeType.TABLE,
            system="databricks",
            metadata={"catalog": catalog, "schema": schema, "table": table}
        )
        self.graph.add_node(table_node)
        try:
            lineage_data = self.client.catalog.table_lineage.get(
                table_name=table,
                catalog_name=catalog,
                schema_name=schema
            )
            if hasattr(lineage_data, 'upstreams'):
                for upstream in lineage_data.upstreams or []:
                    upstream_id = f"{upstream.catalog_name}.{upstream.schema_name}.{upstream.name}"
                    upstream_node = LineageNode(
                        id=upstream_id,
                        node_type=NodeType.TABLE,
                        system="databricks",
                        metadata={"catalog": upstream.catalog_name, "schema": upstream.schema_name, "table": upstream.name}
                    )
                    self.graph.add_node(upstream_node)
                    self.graph.add_edge(upstream_id, table_id)
        except Exception:
            pass
        return self.analyzer.get_cross_system_lineage(table_id)

    def load_external_lineage(self, config_path: str):
        self.integrator.load_from_config(config_path)

    def get_impact_analysis(self, node_id: str, depth: int = -1) -> Dict:
        return self.analyzer.analyze_downstream_impact(node_id, depth)

    def get_dependency_analysis(self, node_id: str, depth: int = -1) -> Dict:
        return self.analyzer.analyze_upstream_dependencies(node_id, depth)

    def handle_table_rename(self, old_table_id: str, new_table_id: str):
        self.analyzer.handle_table_rename(old_table_id, new_table_id)
