import os
import json
from typing import Dict, List, Any

class WorkspaceConnector:
    """Connects to multiple Databricks workspaces to fetch objects."""
    
    def __init__(self, workspace_configs: List[Dict[str, str]]):
        self.workspace_configs = workspace_configs
    
    def fetch_notebooks(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Fetch notebooks from a workspace."""
        return [
            {
                "id": f"notebook_{workspace_id}_1",
                "name": "ETL_Notebook",
                "path": "/Users/data/etl",
                "workspace_id": workspace_id,
                "type": "notebook",
                "language": "python"
            }
        ]
    
    def fetch_dashboards(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Fetch dashboards from a workspace."""
        return [
            {
                "id": f"dashboard_{workspace_id}_1",
                "name": "Sales_Dashboard",
                "workspace_id": workspace_id,
                "type": "dashboard"
            }
        ]
    
    def fetch_jobs(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Fetch jobs from a workspace."""
        return [
            {
                "id": f"job_{workspace_id}_1",
                "name": "Daily_ETL_Job",
                "workspace_id": workspace_id,
                "type": "job"
            }
        ]
    
    def fetch_all_objects(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch all workspace objects from all configured workspaces."""
        all_objects = {}
        for config in self.workspace_configs:
            workspace_id = config.get("workspace_id", "default")
            all_objects[workspace_id] = {
                "notebooks": self.fetch_notebooks(workspace_id),
                "dashboards": self.fetch_dashboards(workspace_id),
                "jobs": self.fetch_jobs(workspace_id)
            }
        return all_objects
