from typing import Dict, List, Optional
from scripts.lineage_graph import LineageGraph, LineageNode, NodeType
from scripts.databricks_mock import WorkspaceClient


class CrossSystemLineage:
    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        self.graph = LineageGraph()
        self.workspace_client = workspace_client or WorkspaceClient()

    def add_table(self, table_name: str, metadata: Optional[Dict] = None):
        node = LineageNode(
            id=table_name,
            name=table_name,
            node_type=NodeType.TABLE,
            metadata=metadata or {}
        )
        self.graph.add_node(node)

    def add_view(self, view_name: str, metadata: Optional[Dict] = None):
        node = LineageNode(
            id=view_name,
            name=view_name,
            node_type=NodeType.VIEW,
            metadata=metadata or {}
        )
        self.graph.add_node(node)

    def add_file(self, file_path: str, metadata: Optional[Dict] = None):
        node = LineageNode(
            id=file_path,
            name=file_path,
            node_type=NodeType.FILE,
            metadata=metadata or {}
        )
        self.graph.add_node(node)

    def add_dependency(self, source_id: str, target_id: str):
        self.graph.add_edge(source_id, target_id)

    def get_impact_analysis(self, asset_id: str) -> Dict:
        node = self.graph.get_node(asset_id)
        if not node:
            return {"error": "Asset not found"}

        downstream = self.graph.get_downstream(asset_id)
        upstream = self.graph.get_upstream(asset_id)

        return {
            "asset": asset_id,
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
            "upstream": upstream,
            "downstream": downstream
        }

    def handle_table_rename(self, old_name: str, new_name: str):
        node = self.graph.get_node(old_name)
        if node:
            new_node = LineageNode(
                id=new_name,
                name=new_name,
                node_type=node.node_type,
                metadata=node.metadata
            )
            self.graph.add_node(new_node)
            for child in self.graph.edges.get(old_name, []):
                self.graph.add_edge(new_name, child)
            for parent in self.graph.reverse_edges.get(old_name, []):
                self.graph.add_edge(parent, new_name)
