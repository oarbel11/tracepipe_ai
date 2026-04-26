from typing import Dict, List, Optional
from scripts.lineage_graph import LineageGraph, LineageNode
import os
import json

class ExternalLineageIntegrator:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def ingest_file_lineage(self, file_path: str, target_table: str, 
                           system: str = "file_system") -> None:
        file_node = LineageNode(
            node_id=file_path,
            node_type="file",
            system=system,
            metadata={"path": file_path, "exists": os.path.exists(file_path)}
        )
        self.graph.add_node(file_node)
        self.graph.add_edge(file_path, target_table, 
                          {"operation": "file_load", "system": system})

    def ingest_bi_lineage(self, dashboard_id: str, source_tables: List[str],
                         bi_tool: str = "powerbi") -> None:
        dashboard_node = LineageNode(
            node_id=dashboard_id,
            node_type="dashboard",
            system=bi_tool,
            metadata={"tool": bi_tool}
        )
        self.graph.add_node(dashboard_node)
        for table in source_tables:
            self.graph.add_edge(table, dashboard_id, 
                              {"operation": "bi_query", "tool": bi_tool})

    def ingest_etl_lineage(self, job_id: str, source_tables: List[str],
                          target_tables: List[str], etl_tool: str = "spark") -> None:
        job_node = LineageNode(
            node_id=job_id,
            node_type="job",
            system=etl_tool,
            metadata={"tool": etl_tool}
        )
        self.graph.add_node(job_node)
        for source in source_tables:
            self.graph.add_edge(source, job_id, 
                              {"operation": "read", "tool": etl_tool})
        for target in target_tables:
            self.graph.add_edge(job_id, target, 
                              {"operation": "write", "tool": etl_tool})

    def ingest_from_config(self, config: Dict) -> None:
        if "files" in config:
            for file_lineage in config["files"]:
                self.ingest_file_lineage(
                    file_lineage["path"],
                    file_lineage["target_table"],
                    file_lineage.get("system", "file_system")
                )
        if "bi_dashboards" in config:
            for dashboard in config["bi_dashboards"]:
                self.ingest_bi_lineage(
                    dashboard["id"],
                    dashboard["source_tables"],
                    dashboard.get("tool", "powerbi")
                )
        if "etl_jobs" in config:
            for job in config["etl_jobs"]:
                self.ingest_etl_lineage(
                    job["id"],
                    job["sources"],
                    job["targets"],
                    job.get("tool", "spark")
                )
