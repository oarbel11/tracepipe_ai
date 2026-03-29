"""Tests for cross-workspace lineage unification."""
import pytest
from src.tracepipe_ai.lineage_unification import (
    LineageUnifier,
    WorkspaceConfig,
    UnifiedLineageGraph,
    LineageNode,
    LineageEdge,
)


def test_workspace_config_creation():
    """Test WorkspaceConfig dataclass creation."""
    config = WorkspaceConfig(
        workspace_id="ws1",
        host="https://test.databricks.com",
        token="test_token",
        metastore_id="metastore1",
    )
    assert config.workspace_id == "ws1"
    assert config.host == "https://test.databricks.com"
    assert config.metastore_id == "metastore1"


def test_lineage_node_creation():
    """Test LineageNode creation."""
    node = LineageNode(
        fqn="catalog.schema.table",
        workspace_id="ws1",
        metastore_id="ms1",
        object_type="table",
    )
    assert node.fqn == "catalog.schema.table"
    assert node.workspace_id == "ws1"
    assert node.object_type == "table"


def test_unified_lineage_graph_add_node():
    """Test adding nodes to unified graph."""
    graph = UnifiedLineageGraph()
    node = LineageNode(
        fqn="catalog.schema.table",
        workspace_id="ws1",
        metastore_id="ms1",
        object_type="table",
    )
    graph.add_node(node)
    assert "catalog.schema.table" in graph.nodes
    assert len(graph.nodes) == 1


def test_unified_lineage_graph_add_edge():
    """Test adding edges to unified graph."""
    graph = UnifiedLineageGraph()
    edge = LineageEdge(
        source_fqn="catalog.schema.source",
        target_fqn="catalog.schema.target",
    )
    graph.add_edge(edge)
    assert len(graph.edges) == 1
    assert graph.edges[0].source_fqn == "catalog.schema.source"


def test_lineage_graph_navigation():
    """Test upstream/downstream navigation."""
    graph = UnifiedLineageGraph()
    graph.add_edge(LineageEdge("table_a", "table_b"))
    graph.add_edge(LineageEdge("table_b", "table_c"))

    upstream = graph.get_upstream("table_b")
    downstream = graph.get_downstream("table_b")

    assert "table_a" in upstream
    assert "table_c" in downstream
