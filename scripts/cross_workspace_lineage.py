from typing import Dict, List, Optional, Set
import yaml
from databricks import sql
import networkx as nx

class CrossWorkspaceLineage:
    def __init__(self, config_path: str = 'config/config.yml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.workspaces = self.config.get('databricks_workspaces', {})
        self.lineage_cache = {}

    def _get_connection(self, workspace_name: str):
        ws_config = self.workspaces.get(workspace_name, {})
        return sql.connect(
            server_hostname=ws_config.get('hostname'),
            http_path=ws_config.get('http_path'),
            access_token=ws_config.get('token')
        )

    def fetch_workspace_lineage(self, workspace_name: str) -> Dict[str, Any]:
        if workspace_name in self.lineage_cache:
            return self.lineage_cache[workspace_name]
        conn = self._get_connection(workspace_name)
        lineage = {'tables': [], 'notebooks': [], 'columns': []}
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT table_catalog, table_schema, table_name FROM system.information_schema.tables")
            lineage['tables'] = [{'catalog': r[0], 'schema': r[1], 'name': r[2]} for r in cursor.fetchall()]
            cursor.close()
        except Exception as e:
            lineage['error'] = str(e)
        finally:
            conn.close()
        self.lineage_cache[workspace_name] = lineage
        return lineage

    def get_table_lineage(self, table_fqn: str, include_workspaces: List[str]) -> Dict[str, Any]:
        graph = nx.DiGraph()
        for ws in include_workspaces:
            ws_lineage = self.fetch_workspace_lineage(ws)
            for table in ws_lineage.get('tables', []):
                fqn = f"{table['catalog']}.{table['schema']}.{table['name']}"
                graph.add_node(fqn, workspace=ws, type='table')
        upstream = list(graph.predecessors(table_fqn)) if table_fqn in graph else []
        downstream = list(graph.successors(table_fqn)) if table_fqn in graph else []
        return {'table': table_fqn, 'upstream': upstream, 'downstream': downstream, 'graph': nx.node_link_data(graph)}

    def aggregate_lineage(self, include_workspaces: List[str]) -> Dict[str, Any]:
        all_lineage = {}
        for ws in include_workspaces:
            all_lineage[ws] = self.fetch_workspace_lineage(ws)
        return all_lineage
