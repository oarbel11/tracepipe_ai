from databricks import sql
import networkx as nx
from typing import Dict, List, Tuple
from config.workspace_config import WorkspaceConfig

class LineageIngestion:
    def __init__(self, workspace_config: Dict):
        self.workspace = workspace_config
        self.name = workspace_config['name']
        self.metastore_id = workspace_config['metastore_id']

    def fetch_lineage(self) -> List[Dict]:
        lineage_data = []
        try:
            with sql.connect(
                server_hostname=self.workspace['host'],
                http_path='/sql/1.0/warehouses/default',
                access_token=self.workspace['token']
            ) as connection:
                with connection.cursor() as cursor:
                    query = """SELECT table_catalog, table_schema, table_name, 
                               upstream_table_name, upstream_table_catalog
                               FROM system.access.table_lineage"""
                    cursor.execute(query)
                    for row in cursor.fetchall():
                        lineage_data.append({
                            'workspace': self.name,
                            'metastore': self.metastore_id,
                            'catalog': row[0],
                            'schema': row[1],
                            'table': row[2],
                            'upstream_table': row[3],
                            'upstream_catalog': row[4]
                        })
        except Exception as e:
            print(f"Error fetching lineage from {self.name}: {e}")
        return lineage_data

class LineageUnifier:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.entity_map = {}

    def add_lineage_data(self, lineage_data: List[Dict]):
        for edge in lineage_data:
            source = self._create_entity_id(edge, is_upstream=True)
            target = self._create_entity_id(edge, is_upstream=False)
            self.graph.add_edge(source, target, **edge)
            self._update_entity_map(source, edge, is_upstream=True)
            self._update_entity_map(target, edge, is_upstream=False)

    def _create_entity_id(self, edge: Dict, is_upstream: bool) -> str:
        if is_upstream:
            return f"{edge['workspace']}.{edge['upstream_catalog']}.{edge['upstream_table']}"
        return f"{edge['workspace']}.{edge['catalog']}.{edge['schema']}.{edge['table']}"

    def _update_entity_map(self, entity_id: str, edge: Dict, is_upstream: bool):
        if entity_id not in self.entity_map:
            self.entity_map[entity_id] = edge.copy()

    def get_unified_graph(self) -> nx.DiGraph:
        return self.graph

    def find_cross_workspace_lineage(self, entity_pattern: str) -> List[Tuple]:
        matches = [node for node in self.graph.nodes if entity_pattern in node]
        paths = []
        for node in matches:
            for successor in self.graph.successors(node):
                paths.append((node, successor))
        return paths
