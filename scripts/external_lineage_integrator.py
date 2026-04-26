from typing import Dict, List
from scripts.lineage_graph import LineageGraph, LineageNode, NodeType
import os
import json

class ExternalLineageIntegrator:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def integrate_file_lineage(self, file_path: str, source_tables: List[str]):
        file_node = LineageNode(
            id=f"file://{file_path}",
            node_type=NodeType.FILE,
            system="filesystem",
            metadata={"path": file_path}
        )
        self.graph.add_node(file_node)
        for table in source_tables:
            self.graph.add_edge(table, file_node.id)

    def integrate_bi_lineage(self, report_id: str, source_tables: List[str], bi_system: str = "powerbi"):
        bi_node = LineageNode(
            id=f"{bi_system}://{report_id}",
            node_type=NodeType.BI_REPORT,
            system=bi_system,
            metadata={"report_id": report_id}
        )
        self.graph.add_node(bi_node)
        for table in source_tables:
            self.graph.add_edge(table, bi_node.id)

    def integrate_etl_lineage(self, job_id: str, source_tables: List[str], target_tables: List[str], etl_system: str = "airflow"):
        etl_node = LineageNode(
            id=f"{etl_system}://{job_id}",
            node_type=NodeType.ETL_JOB,
            system=etl_system,
            metadata={"job_id": job_id}
        )
        self.graph.add_node(etl_node)
        for source in source_tables:
            self.graph.add_edge(source, etl_node.id)
        for target in target_tables:
            self.graph.add_edge(etl_node.id, target)

    def load_from_config(self, config_path: str):
        if not os.path.exists(config_path):
            return
        with open(config_path, 'r') as f:
            config = json.load(f)
        for file_lineage in config.get('files', []):
            self.integrate_file_lineage(file_lineage['path'], file_lineage['sources'])
        for bi_lineage in config.get('bi_reports', []):
            self.integrate_bi_lineage(bi_lineage['id'], bi_lineage['sources'], bi_lineage.get('system', 'powerbi'))
        for etl_lineage in config.get('etl_jobs', []):
            self.integrate_etl_lineage(etl_lineage['id'], etl_lineage['sources'], etl_lineage['targets'], etl_lineage.get('system', 'airflow'))
