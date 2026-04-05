"""Tests for lineage unification."""
import pytest
import json
from src.lineage_unifier import LineageUnifier, LineageNode, LineageEdge
from src.connectors.dbt_connector import DbtConnector


def test_lineage_node_creation():
    node = LineageNode("node1", "table", "databricks", {"catalog": "main"})
    assert node.node_id == "node1"
    assert node.node_type == "table"
    assert node.system == "databricks"
    assert node.metadata["catalog"] == "main"


def test_lineage_edge_creation():
    edge = LineageEdge("node1", "node2", "derives_from")
    assert edge.source_id == "node1"
    assert edge.target_id == "node2"
    assert edge.edge_type == "derives_from"


def test_lineage_unifier_basic():
    unifier = LineageUnifier()
    node1 = LineageNode("table1", "table", "databricks")
    node2 = LineageNode("table2", "table", "databricks")
    edge = LineageEdge("table1", "table2")
    
    unifier.add_node(node1)
    unifier.add_node(node2)
    unifier.add_edge(edge)
    
    lineage = unifier.get_unified_lineage()
    assert len(lineage["nodes"]) == 2
    assert len(lineage["edges"]) == 1


def test_lineage_merge():
    unifier = LineageUnifier()
    external = {
        "nodes": [{"node_id": "ext1", "node_type": "model", "system": "dbt"}],
        "edges": [{"source_id": "ext1", "target_id": "ext2", "edge_type": "derives_from"}]
    }
    unifier.merge_lineage(external)
    lineage = unifier.get_unified_lineage()
    assert len(lineage["nodes"]) == 1
    assert len(lineage["edges"]) == 1


def test_dbt_connector_validation():
    connector = DbtConnector({"manifest_path": "/nonexistent/path"})
    assert connector.validate_connection() == False


def test_dbt_connector_fetch_empty():
    connector = DbtConnector({})
    lineage = connector.fetch_lineage()
    assert lineage == {"nodes": [], "edges": []}
