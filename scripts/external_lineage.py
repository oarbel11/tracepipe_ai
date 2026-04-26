from typing import Dict, List, Optional, Any
from scripts.lineage_graph import LineageGraph

class ExternalLineageIntegrator:
    def __init__(self, graph: LineageGraph):
        self.graph = graph
        self.external_sources: Dict[str, Dict] = {}

    def add_external_source(self, source_id: str, source_type: str, 
                           config: Dict[str, Any]):
        self.external_sources[source_id] = {
            'type': source_type,
            'config': config
        }

    def integrate_etl_lineage(self, etl_tool: str, 
                             lineage_data: List[Dict]):
        for item in lineage_data:
            source_id = item.get('source_id')
            target_id = item.get('target_id')
            if source_id and target_id:
                if source_id not in self.graph.nodes:
                    self.graph.add_node(
                        source_id, 'external', 
                        item.get('source_name', source_id),
                        {'etl_tool': etl_tool}
                    )
                if target_id not in self.graph.nodes:
                    self.graph.add_node(
                        target_id, 'table', 
                        item.get('target_name', target_id),
                        {'etl_tool': etl_tool}
                    )
                self.graph.add_edge(source_id, target_id)

    def integrate_bi_lineage(self, bi_tool: str, lineage_data: List[Dict]):
        for item in lineage_data:
            source_id = item.get('source_id')
            report_id = item.get('report_id')
            if source_id and report_id:
                if report_id not in self.graph.nodes:
                    self.graph.add_node(
                        report_id, 'bi_report',
                        item.get('report_name', report_id),
                        {'bi_tool': bi_tool}
                    )
                self.graph.add_edge(source_id, report_id)

    def integrate_file_lineage(self, file_path: str, table_id: str, 
                              operation: str):
        file_node_id = f"file:{file_path}"
        if file_node_id not in self.graph.nodes:
            self.graph.add_node(file_node_id, 'file', file_path, 
                              {'path': file_path})
        if operation == 'read':
            self.graph.add_edge(file_node_id, table_id)
        elif operation == 'write':
            self.graph.add_edge(table_id, file_node_id)
