"""dbt lineage connector implementation."""
import json
from pathlib import Path
from typing import Dict, Any, List
from connectors import BaseLineageConnector, LineageNode, LineageEdge, ConnectorRegistry


class DbtConnector(BaseLineageConnector):
    """Connector for extracting lineage from dbt projects."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.manifest_path = config.get("manifest_path", "target/manifest.json")

    def extract_lineage(self) -> Dict[str, Any]:
        """Extract lineage from dbt manifest.json."""
        nodes = []
        edges = []

        manifest_file = Path(self.manifest_path)
        if not manifest_file.exists():
            return {"nodes": nodes, "edges": edges}

        with open(manifest_file, "r") as f:
            manifest = json.load(f)

        dbt_nodes = manifest.get("nodes", {})
        for node_id, node_data in dbt_nodes.items():
            node = LineageNode(
                id=f"dbt:{node_id}",
                name=node_data.get("name", node_id),
                type=node_data.get("resource_type", "model"),
                system="dbt",
                metadata={
                    "schema": node_data.get("schema"),
                    "database": node_data.get("database"),
                    "path": node_data.get("path")
                }
            )
            nodes.append(node)

            for dep in node_data.get("depends_on", {}).get("nodes", []):
                edge = LineageEdge(
                    source_id=f"dbt:{dep}",
                    target_id=f"dbt:{node_id}",
                    edge_type="transforms"
                )
                edges.append(edge)

        return {
            "nodes": [n.to_dict() for n in nodes],
            "edges": [e.to_dict() for e in edges]
        }


ConnectorRegistry.register("dbt", DbtConnector)
