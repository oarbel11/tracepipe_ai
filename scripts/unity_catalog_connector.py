import os
from typing import Dict, List, Any, Optional
from databricks import sql
import yaml


class UnityCatalogConnector:
    def __init__(self, config_path: str = "config/config.yml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.workspaces = self.config.get('databricks', {}).get('workspaces', [])
        self.connections = {}

    def _get_connection(self, workspace_id: str):
        if workspace_id not in self.connections:
            ws_config = next((w for w in self.workspaces if w['id'] == workspace_id), None)
            if not ws_config:
                raise ValueError(f"Workspace {workspace_id} not configured")
            self.connections[workspace_id] = sql.connect(
                server_hostname=ws_config['hostname'],
                http_path=ws_config['http_path'],
                access_token=ws_config.get('token', os.getenv('DATABRICKS_TOKEN'))
            )
        return self.connections[workspace_id]

    def fetch_workspace_objects(self, workspace_id: str) -> List[Dict[str, Any]]:
        conn = self._get_connection(workspace_id)
        cursor = conn.cursor()
        objects = []
        
        queries = {
            'notebooks': """SELECT object_id, path, created_by, modified_time, 
                           object_type FROM system.information_schema.notebooks""",
            'dashboards': """SELECT dashboard_id, name, created_by, updated_time
                            FROM system.dashboards""",
            'jobs': """SELECT job_id, name, creator, created_time 
                      FROM system.jobs"""
        }
        
        for obj_type, query in queries.items():
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    objects.append({
                        'workspace_id': workspace_id,
                        'type': obj_type,
                        'data': dict(zip([col[0] for col in cursor.description], row))
                    })
            except Exception as e:
                print(f"Error fetching {obj_type} from {workspace_id}: {e}")
        
        cursor.close()
        return objects

    def fetch_all_workspaces(self) -> Dict[str, List[Dict[str, Any]]]:
        all_objects = {}
        for workspace in self.workspaces:
            ws_id = workspace['id']
            all_objects[ws_id] = self.fetch_workspace_objects(ws_id)
        return all_objects

    def close_all(self):
        for conn in self.connections.values():
            conn.close()
