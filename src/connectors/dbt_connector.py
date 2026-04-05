"""dbt lineage connector."""
import json
from typing import Dict, Any, List
from .base_connector import BaseLineageConnector


class DbtConnector(BaseLineageConnector):
    """Connector for dbt lineage metadata."""

    def validate_connection(self) -> bool:
        manifest_path = self.config.get("manifest_path")
        if not manifest_path:
            return False
        try:
            with open(manifest_path, "r") as f:
                json.load(f)
            return True
        except Exception:
            return False

    def fetch_lineage(self) -> Dict[str, Any]:
        manifest_path = self.config.get("manifest_path")
        if not manifest_path:
            return {"nodes": [], "edges": []}

        try:
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
        except Exception:
            return {"nodes": [], "edges": []}

        nodes = []
        edges = []

        for node_id, node_data in manifest.get("nodes", {}).items():
            nodes.append({
                "node_id": node_id,
                "node_type": node_data.get("resource_type", "model"),
                "system": "dbt",
                "metadata": {
                    "name": node_data.get("name"),
                    "database": node_data.get("database"),
                    "schema": node_data.get("schema")
                }
            })

            for dep in node_data.get("depends_on", {}).get("nodes", []):
                edges.append({
                    "source_id": dep,
                    "target_id": node_id,
                    "edge_type": "derives_from"
                })

        return {"nodes": nodes, "edges": edges}
