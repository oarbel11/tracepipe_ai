"""Cross-workspace and cross-metastore lineage unification."""
from typing import List, Dict, Set, Optional, Any
from dataclasses import dataclass, field
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import LineageColumnInfo, ColumnLineage
import logging

logger = logging.getLogger(__name__)


@dataclass
class WorkspaceConfig:
    """Configuration for a Databricks workspace."""
    workspace_id: str
    host: str
    token: str
    metastore_id: Optional[str] = None


@dataclass
class LineageNode:
    """Represents a node in the unified lineage graph."""
    fqn: str
    workspace_id: str
    metastore_id: Optional[str]
    object_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    """Represents an edge in the unified lineage graph."""
    source_fqn: str
    target_fqn: str
    edge_type: str = "depends_on"


class UnifiedLineageGraph:
    """Unified lineage graph across workspaces."""
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.fqn] = node

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)

    def get_upstream(self, fqn: str) -> List[str]:
        return [e.source_fqn for e in self.edges if e.target_fqn == fqn]

    def get_downstream(self, fqn: str) -> List[str]:
        return [e.target_fqn for e in self.edges if e.source_fqn == fqn]


class LineageUnifier:
    """Unifies lineage from multiple Databricks workspaces."""
    def __init__(self, workspace_configs: List[WorkspaceConfig]):
        self.workspace_configs = workspace_configs
        self.clients: Dict[str, WorkspaceClient] = {}
        self._initialize_clients()

    def _initialize_clients(self) -> None:
        for config in self.workspace_configs:
            self.clients[config.workspace_id] = WorkspaceClient(
                host=config.host, token=config.token
            )

    def fetch_lineage(self) -> UnifiedLineageGraph:
        graph = UnifiedLineageGraph()
        for config in self.workspace_configs:
            self._fetch_workspace_lineage(config, graph)
        return graph

    def _fetch_workspace_lineage(self, config: WorkspaceConfig,
                                  graph: UnifiedLineageGraph) -> None:
        client = self.clients[config.workspace_id]
        try:
            tables = client.tables.list(catalog_name="*", schema_name="*")
            for table in tables:
                fqn = f"{table.catalog_name}.{table.schema_name}.{table.name}"
                node = LineageNode(
                    fqn=fqn, workspace_id=config.workspace_id,
                    metastore_id=config.metastore_id,
                    object_type="table"
                )
                graph.add_node(node)
        except Exception as e:
            logger.warning(f"Error fetching lineage: {e}")
