import json
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional


class LineageConnector:
    """Base class for lineage connectors"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def extract_lineage(self) -> Dict[str, Any]:
        """Extract lineage from external system"""
        raise NotImplementedError


class SnowflakeConnector(LineageConnector):
    """Snowflake lineage connector"""

    def extract_lineage(self) -> Dict[str, Any]:
        return {
            "source": "snowflake",
            "nodes": [
                {"id": "sf.db.table1", "type": "table", "system": "snowflake"},
                {"id": "sf.db.table2", "type": "table", "system": "snowflake"}
            ],
            "edges": [
                {"from": "sf.db.table1", "to": "sf.db.table2", "type": "dataflow"}
            ]
        }


class TableauConnector(LineageConnector):
    """Tableau lineage connector"""

    def extract_lineage(self) -> Dict[str, Any]:
        return {
            "source": "tableau",
            "nodes": [
                {"id": "tableau.dashboard1", "type": "dashboard", "system": "tableau"}
            ],
            "edges": []
        }


class LineageStitcher:
    """Stitches lineage graphs from multiple sources"""

    def __init__(self):
        self.nodes = {}
        self.edges = []

    def add_lineage(self, lineage: Dict[str, Any]):
        """Add lineage data from a source"""
        for node in lineage.get("nodes", []):
            self.nodes[node["id"]] = node
        for edge in lineage.get("edges", []):
            self.edges.append(edge)

    def get_graph(self) -> Dict[str, Any]:
        """Return unified lineage graph"""
        return {"nodes": list(self.nodes.values()), "edges": self.edges}


class LineageEngine:
    """Main engine for lineage integration"""

    def __init__(self, config_path: str = "config/config.yml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.connectors = []
        self.stitcher = LineageStitcher()

    def register_connector(self, connector: LineageConnector):
        """Register a lineage connector"""
        self.connectors.append(connector)

    def collect_lineage(self):
        """Collect lineage from all connectors"""
        for connector in self.connectors:
            lineage = connector.extract_lineage()
            self.stitcher.add_lineage(lineage)

    def query_lineage(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Query lineage for a specific node"""
        return self.stitcher.nodes.get(node_id)

    def get_unified_graph(self) -> Dict[str, Any]:
        """Get the complete unified lineage graph"""
        return self.stitcher.get_graph()
