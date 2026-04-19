from typing import Dict, List, Any
from scripts.unified_lineage import UnifiedLineageGraph, LineageNode


class LineageAggregator:
    """Aggregates lineage from multiple sources into unified graph"""

    def __init__(self):
        self.unified_graph = UnifiedLineageGraph()
        self.connectors = {}

    def register_connector(self, platform: str, connector: 'ExternalLineageConnector'):
        """Register external lineage connector"""
        self.connectors[platform] = connector

    def aggregate_lineage(self) -> UnifiedLineageGraph:
        """Aggregate lineage from all registered connectors"""
        for platform, connector in self.connectors.items():
            external_lineage = connector.fetch_lineage()
            self.unified_graph.merge_lineage(external_lineage)
        return self.unified_graph

    def add_unity_catalog_lineage(self, uc_lineage: Dict[str, Any]):
        """Add Unity Catalog lineage to unified graph"""
        for item in uc_lineage.get("nodes", []):
            node = LineageNode(item.get("id"), item.get("type", "table"), "unity_catalog", item.get("metadata"))
            self.unified_graph.add_node(node)

        for edge in uc_lineage.get("edges", []):
            self.unified_graph.add_edge(edge["source"], edge["target"])

    def get_unified_graph(self) -> UnifiedLineageGraph:
        """Return the unified lineage graph"""
        return self.unified_graph


class ExternalLineageConnector:
    """Base class for external lineage connectors"""

    def __init__(self, platform_name: str):
        self.platform_name = platform_name

    def fetch_lineage(self) -> Dict[str, Any]:
        """Fetch lineage from external platform (override in subclass)"""
        return {"nodes": [], "edges": []}


class BIToolConnector(ExternalLineageConnector):
    """Connector for BI tools like Tableau, PowerBI"""

    def __init__(self, platform_name: str, api_endpoint: str):
        super().__init__(platform_name)
        self.api_endpoint = api_endpoint

    def fetch_lineage(self) -> Dict[str, Any]:
        """Fetch lineage from BI tool"""
        return {"nodes": [{"id": f"{self.platform_name}_dashboard", "type": "dashboard", "platform": self.platform_name}], "edges": []}
