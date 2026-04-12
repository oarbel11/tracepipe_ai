import json
from datetime import datetime
from typing import Dict, List, Set, Optional


class LineageNode:
    def __init__(self, node_id: str, system: str, entity_type: str,
                 name: str, metadata: Optional[Dict] = None):
        self.node_id = node_id
        self.system = system
        self.entity_type = entity_type
        self.name = name
        self.metadata = metadata or {}


class LineageEdge:
    def __init__(self, source_id: str, target_id: str,
                 edge_type: str = "derives_from"):
        self.source_id = source_id
        self.target_id = target_id
        self.edge_type = edge_type


class LineageGraph:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []

    def add_node(self, node: LineageNode):
        self.nodes[node.node_id] = node

    def add_edge(self, edge: LineageEdge):
        self.edges.append(edge)

    def get_upstream(self, node_id: str) -> List[str]:
        return [e.source_id for e in self.edges if e.target_id == node_id]

    def get_downstream(self, node_id: str) -> List[str]:
        return [e.target_id for e in self.edges if e.source_id == node_id]


class BaseConnector:
    def __init__(self, config: Dict):
        self.config = config
        self.system_name = config.get("system", "unknown")

    def extract_lineage(self) -> LineageGraph:
        raise NotImplementedError


class SnowflakeConnector(BaseConnector):
    def extract_lineage(self) -> LineageGraph:
        graph = LineageGraph()
        tables = self.config.get("tables", [])
        for table in tables:
            node = LineageNode(
                node_id=f"snowflake:{table['name']}",
                system="snowflake",
                entity_type="table",
                name=table["name"],
                metadata={"schema": table.get("schema", "")}
            )
            graph.add_node(node)
        return graph


class TableauConnector(BaseConnector):
    def extract_lineage(self) -> LineageGraph:
        graph = LineageGraph()
        workbooks = self.config.get("workbooks", [])
        for wb in workbooks:
            node = LineageNode(
                node_id=f"tableau:{wb['name']}",
                system="tableau",
                entity_type="workbook",
                name=wb["name"]
            )
            graph.add_node(node)
        return graph
