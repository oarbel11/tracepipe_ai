from typing import Dict, List, Any, Optional
from scripts.lineage_graph import LineageGraph, LineageNode


class SparkLineageExtractor:
    def __init__(self, databricks_client):
        self.client = databricks_client
        self.graph = LineageGraph()

    def extract_lineage(self, table_name: str) -> LineageGraph:
        table_id = f"table:{table_name}"
        self.graph.add_node(table_id, 'table', table_name, {})
        
        lineage_info = self.client.get_table_lineage(table_name)
        
        for upstream in lineage_info.get('upstream_tables', []):
            upstream_id = f"table:{upstream}"
            self.graph.add_node(upstream_id, 'table', upstream, {})
            self.graph.add_edge(upstream_id, table_id)
        
        for downstream in lineage_info.get('downstream_tables', []):
            downstream_id = f"table:{downstream}"
            self.graph.add_node(downstream_id, 'table', downstream, {})
            self.graph.add_edge(table_id, downstream_id)
        
        return self.graph

    def integrate_external_etl(self, etl_metadata: Dict[str, Any]) -> None:
        etl_id = f"etl:{etl_metadata['job_name']}"
        self.graph.add_node(etl_id, 'etl_job', etl_metadata['job_name'],
                           etl_metadata)
        
        for source in etl_metadata.get('sources', []):
            source_id = f"table:{source}"
            if source_id not in self.graph.nodes:
                self.graph.add_node(source_id, 'table', source, {})
            self.graph.add_edge(source_id, etl_id)
        
        for target in etl_metadata.get('targets', []):
            target_id = f"table:{target}"
            if target_id not in self.graph.nodes:
                self.graph.add_node(target_id, 'table', target, {})
            self.graph.add_edge(etl_id, target_id)

    def integrate_bi_tool(self, bi_metadata: Dict[str, Any]) -> None:
        bi_id = f"bi:{bi_metadata['report_name']}"
        self.graph.add_node(bi_id, 'bi_report', bi_metadata['report_name'],
                           bi_metadata)
        
        for source in bi_metadata.get('sources', []):
            source_id = f"table:{source}"
            if source_id not in self.graph.nodes:
                self.graph.add_node(source_id, 'table', source, {})
            self.graph.add_edge(source_id, bi_id)

    def track_file_lineage(self, file_path: str, related_tables: List[str],
                          operation: str) -> None:
        file_id = f"file:{file_path}"
        self.graph.add_node(file_id, 'file', file_path,
                           {'operation': operation})
        
        for table in related_tables:
            table_id = f"table:{table}"
            if table_id not in self.graph.nodes:
                self.graph.add_node(table_id, 'table', table, {})
            if operation == 'write':
                self.graph.add_edge(table_id, file_id)
            else:
                self.graph.add_edge(file_id, table_id)

    def handle_table_rename(self, old_name: str, new_name: str) -> None:
        old_id = f"table:{old_name}"
        new_id = f"table:{new_name}"
        
        if old_id in self.graph.nodes:
            old_node = self.graph.nodes[old_id]
            self.graph.add_node(new_id, old_node.node_type, new_name,
                               old_node.metadata)
