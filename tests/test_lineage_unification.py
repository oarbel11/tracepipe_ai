"""Tests for cross-workspace lineage unification."""
import pytest
from src.tracepipe_ai.lineage_unification import (
    LineageNode,
    LineageEdge,
    UnifiedLineageGraph,
    LineageUnifier,
)


def test_lineage_node_creation():
    """Test creating a lineage node."""
    node = LineageNode(
        id="node1",
        workspace="ws1",
        metastore="meta1",
        catalog="catalog1",
        schema="schema1",
        table="table1"
    )
    assert node.id == "node1"
    assert node.get_fqn() == "catalog1.schema1.table1"


def test_lineage_edge_creation():
    """Test creating a lineage edge."""
    edge = LineageEdge(source="node1", target="node2")
    assert edge.source == "node1"
    assert edge.target == "node2"


def test_unified_graph_add_node():
    """Test adding nodes to unified graph."""
    graph = UnifiedLineageGraph()
    node = LineageNode(
        id="node1",
        workspace="ws1",
        metastore="meta1",
        catalog="cat1",
        schema="sch1",
        table="tbl1"
    )
    graph.add_node(node)
    assert "node1" in graph.nodes
    assert graph.nodes["node1"].table == "tbl1"


def test_unified_graph_add_edge():
    """Test adding edges to unified graph."""
    graph = UnifiedLineageGraph()
    edge = LineageEdge(source="node1", target="node2")
    graph.add_edge(edge)
    assert len(graph.edges) == 1


def test_unified_graph_get_downstream():
    """Test getting downstream dependencies."""
    graph = UnifiedLineageGraph()
    graph.add_edge(LineageEdge(source="node1", target="node2"))
    graph.add_edge(LineageEdge(source="node1", target="node3"))
    downstream = graph.get_downstream("node1")
    assert len(downstream) == 2
    assert "node2" in downstream
    assert "node3" in downstream


def test_unified_graph_get_upstream():
    """Test getting upstream dependencies."""
    graph = UnifiedLineageGraph()
    graph.add_edge(LineageEdge(source="node1", target="node3"))
    graph.add_edge(LineageEdge(source="node2", target="node3"))
    upstream = graph.get_upstream("node3")
    assert len(upstream) == 2
    assert "node1" in upstream
    assert "node2" in upstream


def test_lineage_unifier_initialization():
    """Test initializing lineage unifier."""
    configs = [{"workspace_id": "ws1", "host": "https://test.databricks.com", "token": "test"}]
    unifier = LineageUnifier(configs)
    assert len(unifier.workspace_configs) == 1
