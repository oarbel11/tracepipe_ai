import pytest
import networkx as nx
from unittest.mock import Mock, patch, MagicMock
from scripts.operational_lineage import OperationalLineageTracker


@pytest.fixture
def mock_config(tmp_path):
    config_file = tmp_path / "config.yml"
    config_file.write_text("""
databricks:
  server_hostname: "test.databricks.com"
  http_path: "/sql/1.0/warehouses/test"
  access_token: "test_token"
""")
    return str(config_file)


@pytest.fixture
def tracker(mock_config):
    return OperationalLineageTracker(config_path=mock_config)


def test_extract_tables_written(tracker):
    query = "CREATE TABLE catalog.schema.table AS SELECT * FROM source"
    tables = tracker._extract_tables_written(query, "CREATE_TABLE")
    assert "catalog.schema.table" in tables


def test_extract_tables_read(tracker):
    query = "SELECT * FROM catalog.schema.source_table"
    tables = tracker._extract_tables_read(query)
    assert "catalog.schema.source_table" in tables


def test_parse_query_lineage(tracker):
    query_row = (
        "INSERT INTO target SELECT * FROM source",
        "user@example.com",
        "INSERT",
        "user@example.com",
        "2024-01-01T12:00:00"
    )
    tracker._parse_query_lineage(query_row)
    assert len(tracker.graph.nodes()) > 0
    code_nodes = [n for n in tracker.graph.nodes() 
                  if tracker.graph.nodes[n].get('node_type') == 'code']
    assert len(code_nodes) == 1


def test_get_upstream_code(tracker):
    tracker.graph.add_node("code1", node_type="code")
    tracker.graph.add_node("table1", node_type="data")
    tracker.graph.add_edge("code1", "table1", relationship="writes")
    tracker.code_assets["code1"] = {"type": "query", "user": "test"}
    
    upstream = tracker.get_upstream_code("table1")
    assert len(upstream) == 1
    assert upstream[0]["user"] == "test"


def test_visualize_graph(tracker, tmp_path):
    tracker.graph.add_node("code1", node_type="code")
    tracker.graph.add_node("table1", node_type="data")
    tracker.graph.add_edge("code1", "table1")
    
    output = tmp_path / "test.html"
    tracker.visualize_graph(str(output))
    
    assert output.exists()
    content = output.read_text()
    assert "Operational Lineage" in content
