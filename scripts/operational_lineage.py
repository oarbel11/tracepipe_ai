import networkx as nx
import json
from typing import Dict, List, Set, Tuple
from databricks import sql
from config.db_config import get_databricks_config


class OperationalLineageExtractor:
    def __init__(self, connection_params: Dict):
        self.conn_params = connection_params
        self.graph = nx.DiGraph()

    def extract_notebook_lineage(self) -> List[Dict]:
        """Extract lineage from notebook execution history."""
        with sql.connect(**self.conn_params) as conn:
            cursor = conn.cursor()
            query = """
            SELECT DISTINCT
                r.notebook_path,
                t.table_catalog,
                t.table_schema,
                t.table_name,
                r.operation_type
            FROM system.access.table_lineage t
            JOIN system.compute.notebook_runs r
                ON t.source_notebook_id = r.notebook_id
            WHERE r.end_time > current_timestamp() - INTERVAL 30 DAYS
            """
            cursor.execute(query)
            return [dict(zip([d[0] for d in cursor.description], row)) 
                    for row in cursor.fetchall()]

    def extract_job_lineage(self) -> List[Dict]:
        """Extract lineage from Spark job executions."""
        with sql.connect(**self.conn_params) as conn:
            cursor = conn.cursor()
            query = """
            SELECT DISTINCT
                j.job_name,
                j.job_id,
                t.table_catalog,
                t.table_schema,
                t.table_name,
                t.operation_type
            FROM system.access.table_lineage t
            JOIN system.compute.jobs j
                ON t.source_job_id = j.job_id
            WHERE j.last_run_time > current_timestamp() - INTERVAL 30 DAYS
            """
            cursor.execute(query)
            return [dict(zip([d[0] for d in cursor.description], row)) 
                    for row in cursor.fetchall()]

    def build_lineage_graph(self) -> nx.DiGraph:
        """Build operational lineage graph with code and data nodes."""
        notebooks = self.extract_notebook_lineage()
        jobs = self.extract_job_lineage()

        for nb in notebooks:
            code_node = f"notebook:{nb['notebook_path']}"
            table_node = f"table:{nb['table_catalog']}.{nb['table_schema']}.{nb['table_name']}"
            self.graph.add_node(code_node, type='notebook', path=nb['notebook_path'])
            self.graph.add_node(table_node, type='table')
            
            if nb['operation_type'] in ['WRITE', 'CREATE', 'INSERT']:
                self.graph.add_edge(code_node, table_node, op=nb['operation_type'])
            else:
                self.graph.add_edge(table_node, code_node, op=nb['operation_type'])

        for job in jobs:
            code_node = f"job:{job['job_name']}"
            table_node = f"table:{job['table_catalog']}.{job['table_schema']}.{job['table_name']}"
            self.graph.add_node(code_node, type='job', job_id=job['job_id'])
            self.graph.add_node(table_node, type='table')
            
            if job['operation_type'] in ['WRITE', 'CREATE', 'INSERT']:
                self.graph.add_edge(code_node, table_node, op=job['operation_type'])
            else:
                self.graph.add_edge(table_node, code_node, op=job['operation_type'])

        return self.graph

    def get_upstream_code(self, table_name: str) -> List[str]:
        """Find all code assets that produce this table."""
        table_node = f"table:{table_name}"
        if table_node not in self.graph:
            return []
        return [n for n in self.graph.predecessors(table_node) 
                if self.graph.nodes[n]['type'] in ['notebook', 'job']]

    def get_downstream_impact(self, code_asset: str) -> List[str]:
        """Find all tables affected by this code asset."""
        if code_asset not in self.graph:
            return []
        return [n for n in nx.descendants(self.graph, code_asset) 
                if self.graph.nodes[n]['type'] == 'table']
