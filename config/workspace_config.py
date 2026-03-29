import yaml
from typing import Dict, List
from pathlib import Path

class WorkspaceConfig:
    def __init__(self, config_path: str = "config/config.yml"):
        self.config_path = config_path
        self.workspaces = self._load_workspaces()

    def _load_workspaces(self) -> List[Dict]:
        config_file = Path(self.config_path)
        if not config_file.exists():
            return []
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config.get('workspaces', [])

    def get_all_workspaces(self) -> List[Dict]:
        return self.workspaces

    def add_workspace(self, name: str, host: str, token: str, 
                      metastore_id: str, workspace_id: str):
        workspace = {
            'name': name,
            'host': host,
            'token': token,
            'metastore_id': metastore_id,
            'workspace_id': workspace_id
        }
        self.workspaces.append(workspace)
        self._save_config()

    def _save_config(self):
        config_file = Path(self.config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if config_file.exists():
            with open(config_file, 'r') as f:
                existing = yaml.safe_load(f) or {}
        existing['workspaces'] = self.workspaces
        with open(config_file, 'w') as f:
            yaml.dump(existing, f, default_flow_style=False)
