from typing import Dict, List, Optional
from scripts.unified_lineage import UnifiedLineageGraph, LineageNode
import json

class ExternalLineageConnector:
    def __init__(self, platform_name: str):
        self.platform_name = platform_name

    def extract_lineage(self, resource_id: str) -> Dict:
        raise NotImplementedError("Subclasses must implement extract_lineage")

class LineageAggregator:
    def __init__(self):
        self.unified_graph = UnifiedLineageGraph()
        self.connectors: Dict[str, ExternalLineageConnector] = {}

    def register_connector(self, connector: ExternalLineageConnector):
        self.connectors[connector.platform_name] = connector

    def add_databricks_lineage(self, table_name: str, upstream: Optional[List[str]] = None):
        node = LineageNode(
            node_id=table_name,
            node_type="table",
            platform="databricks",
            metadata={"catalog_name": table_name.split(".")[0] if "." in table_name else ""}
        )
        self.unified_graph.add_node(node)
        if upstream:
            for upstream_id in upstream:
                upstream_node = LineageNode(
                    node_id=upstream_id,
                    node_type="table",
                    platform="databricks"
                )
                self.unified_graph.add_node(upstream_node)
                self.unified_graph.add_edge(upstream_id, table_name)

    def add_external_lineage(self, platform: str, resource_id: str,
                            upstream: Optional[List[str]] = None,
                            downstream: Optional[List[str]] = None,
                            resource_type: str = "report"):
        node = LineageNode(
            node_id=f"{platform}://{resource_id}",
            node_type=resource_type,
            platform=platform,
            metadata={"resource_id": resource_id}
        )
        self.unified_graph.add_node(node)
        if upstream:
            for upstream_id in upstream:
                if upstream_id not in self.unified_graph.nodes:
                    self.unified_graph.add_node(LineageNode(
                        node_id=upstream_id,
                        node_type="table",
                        platform="databricks"
                    ))
                self.unified_graph.add_edge(upstream_id, node.node_id)
        if downstream:
            for downstream_id in downstream:
                self.unified_graph.add_edge(node.node_id, downstream_id)

    def get_unified_graph(self) -> UnifiedLineageGraph:
        return self.unified_graph

    def export_graph(self, output_path: str):
        with open(output_path, "w") as f:
            json.dump(self.unified_graph.to_dict(), f, indent=2)

    def get_cross_platform_impact(self, node_id: str) -> Dict:
        downstream = self.unified_graph.get_downstream_impact(node_id)
        platforms = {}
        for desc_id in downstream:
            node = self.unified_graph.nodes.get(desc_id)
            if node:
                platforms.setdefault(node.platform, []).append(desc_id)
        return platforms
