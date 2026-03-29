import json
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from databricks.sdk import WorkspaceClient
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    WorkspaceClient = None


class LineageNode:
    def __init__(self, node_id: str, node_type: str, name: str,
                 workspace: str, metastore: str, metadata: Dict):
        self.node_id = node_id
        self.node_type = node_type
        self.name = name
        self.workspace = workspace
        self.metastore = metastore
        self.metadata = metadata

    def to_dict(self):
        return {
            'node_id': self.node_id,
            'node_type': self.node_type,
            'name': self.name,
            'workspace': self.workspace,
            'metastore': self.metastore,
            'metadata': self.metadata
        }


class LineageEdge:
    def __init__(self, source_id: str, target_id: str, edge_type: str):
        self.source_id = source_id
        self.target_id = target_id
        self.edge_type = edge_type

    def to_dict(self):
        return {
            'source_id': self.source_id,
            'target_id': self.target_id,
            'edge_type': self.edge_type
        }


class LineageUnifier:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []

    def ingest_workspace(self, workspace_config: Dict) -> None:
        workspace_name = workspace_config.get('name', 'unknown')
        metastore = workspace_config.get('metastore', 'default')
        lineage_data = workspace_config.get('lineage', {})

        for node_data in lineage_data.get('nodes', []):
            node = LineageNode(
                node_id=f"{workspace_name}:{node_data['id']}",
                node_type=node_data.get('type', 'table'),
                name=node_data.get('name', ''),
                workspace=workspace_name,
                metastore=metastore,
                metadata=node_data.get('metadata', {})
            )
            self.nodes[node.node_id] = node

        for edge_data in lineage_data.get('edges', []):
            edge = LineageEdge(
                source_id=f"{workspace_name}:{edge_data['source']}",
                target_id=f"{workspace_name}:{edge_data['target']}",
                edge_type=edge_data.get('type', 'depends_on')
            )
            self.edges.append(edge)

    def get_unified_graph(self) -> Dict[str, Any]:
        return {
            'nodes': [n.to_dict() for n in self.nodes.values()],
            'edges': [e.to_dict() for e in self.edges]
        }
