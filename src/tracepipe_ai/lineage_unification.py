"""Cross-workspace and cross-metastore lineage unification."""
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass, field
from databricks.sdk import WorkspaceClient


@dataclass
class LineageNode:
    """Represents a node in lineage graph."""
    id: str
    workspace: str
    metastore: str
    catalog: str
    schema: str
    table: str
    node_type: str = "table"

    def get_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass
class LineageEdge:
    """Represents an edge in lineage graph."""
    source: str
    target: str
    edge_type: str = "depends_on"


@dataclass
class UnifiedLineageGraph:
    """Unified lineage graph across workspaces and metastores."""
    nodes: Dict[str, LineageNode] = field(default_factory=dict)
    edges: List[LineageEdge] = field(default_factory=list)

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)

    def get_downstream(self, node_id: str) -> List[str]:
        return [e.target for e in self.edges if e.source == node_id]

    def get_upstream(self, node_id: str) -> List[str]:
        return [e.source for e in self.edges if e.target == node_id]


class LineageUnifier:
    """Unifies lineage across multiple workspaces and metastores."""

    def __init__(self, workspace_configs: List[Dict[str, str]]):
        self.workspace_configs = workspace_configs
        self.clients: Dict[str, WorkspaceClient] = {}
        self.unified_graph = UnifiedLineageGraph()

    def connect_workspaces(self) -> None:
        """Connect to all configured workspaces."""
        for config in self.workspace_configs:
            ws_id = config["workspace_id"]
            self.clients[ws_id] = WorkspaceClient(
                host=config["host"],
                token=config["token"]
            )

    def ingest_lineage(self) -> UnifiedLineageGraph:
        """Ingest lineage from all workspaces."""
        for ws_id, client in self.clients.items():
            self._ingest_from_workspace(ws_id, client)
        return self.unified_graph

    def _ingest_from_workspace(self, ws_id: str, client: WorkspaceClient):
        """Ingest lineage from a single workspace."""
        pass

    def match_entities(self, fqn: str) -> List[str]:
        """Find matching entities across workspaces by FQN."""
        matches = []
        for node_id, node in self.unified_graph.nodes.items():
            if node.get_fqn() == fqn:
                matches.append(node_id)
        return matches
