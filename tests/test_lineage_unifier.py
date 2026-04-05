"""Tests for the lineage unification engine."""
import json
import networkx as nx
from pathlib import Path
from connectors import ConnectorRegistry, LineageNode, LineageEdge
from connectors.dbt_connector import DbtConnector
from scripts.lineage_unifier import LineageUnifier


def test_connector_registry():
    """Test connector registry."""
    assert "dbt" in ConnectorRegistry.list_connectors()
    connector = ConnectorRegistry.get_connector("dbt", {"manifest_path": "fake.json"})
    assert isinstance(connector, DbtConnector)


def test_lineage_node():
    """Test LineageNode creation."""
    node = LineageNode("id1", "table1", "table", "dbt", {"schema": "public"})
    node_dict = node.to_dict()
    assert node_dict["id"] == "id1"
    assert node_dict["name"] == "table1"
    assert node_dict["system"] == "dbt"


def test_lineage_edge():
    """Test LineageEdge creation."""
    edge = LineageEdge("node1", "node2", "transforms")
    edge_dict = edge.to_dict()
    assert edge_dict["source_id"] == "node1"
    assert edge_dict["target_id"] == "node2"


def test_dbt_connector_no_manifest(tmp_path):
    """Test dbt connector when manifest doesn't exist."""
    connector = DbtConnector({"manifest_path": str(tmp_path / "nonexistent.json")})
    lineage = connector.extract_lineage()
    assert lineage["nodes"] == []
    assert lineage["edges"] == []


def test_dbt_connector_with_manifest(tmp_path):
    """Test dbt connector with a valid manifest."""
    manifest = {
        "nodes": {
            "model.project.model1": {
                "name": "model1",
                "resource_type": "model",
                "schema": "public",
                "database": "analytics",
                "path": "models/model1.sql",
                "depends_on": {"nodes": ["source.project.raw_data"]}
            }
        }
    }
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    connector = DbtConnector({"manifest_path": str(manifest_path)})
    lineage = connector.extract_lineage()
    assert len(lineage["nodes"]) == 1
    assert len(lineage["edges"]) == 1


def test_lineage_unifier():
    """Test lineage unifier basic functionality."""
    unifier = LineageUnifier()
    databricks_lineage = {
        "nodes": [{"id": "table1", "name": "table1", "type": "table"}],
        "edges": [{"source_id": "table1", "target_id": "table2"}]
    }
    unifier.add_databricks_lineage(databricks_lineage)
    unified = unifier.get_unified_lineage()
    assert len(unified["nodes"]) >= 1
