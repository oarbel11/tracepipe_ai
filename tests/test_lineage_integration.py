import pytest
import sys
from pathlib import Path
import yaml
import tempfile
import os

from scripts.lineage_integration import (
    LineageEngine,
    SnowflakeConnector,
    TableauConnector,
    LineageStitcher
)


@pytest.fixture
def config_file():
    """Create temporary config file"""
    config = {
        "lineage": {
            "systems": {
                "snowflake": {"enabled": True, "account": "test"},
                "tableau": {"enabled": True, "server": "https://test.com"}
            }
        }
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config, f)
        return f.name


def test_snowflake_connector():
    connector = SnowflakeConnector({"account": "test"})
    lineage = connector.extract_lineage()
    assert lineage["source"] == "snowflake"
    assert len(lineage["nodes"]) > 0
    assert "edges" in lineage


def test_tableau_connector():
    connector = TableauConnector({"server": "https://test.com"})
    lineage = connector.extract_lineage()
    assert lineage["source"] == "tableau"
    assert "nodes" in lineage


def test_lineage_stitcher():
    stitcher = LineageStitcher()
    lineage1 = {
        "nodes": [{"id": "n1", "type": "table"}],
        "edges": [{"from": "n1", "to": "n2"}]
    }
    lineage2 = {
        "nodes": [{"id": "n2", "type": "table"}],
        "edges": []
    }
    stitcher.add_lineage(lineage1)
    stitcher.add_lineage(lineage2)
    graph = stitcher.get_graph()
    assert len(graph["nodes"]) == 2
    assert len(graph["edges"]) == 1


def test_lineage_engine(config_file):
    engine = LineageEngine(config_file)
    sf_connector = SnowflakeConnector({"account": "test"})
    tb_connector = TableauConnector({"server": "https://test.com"})
    engine.register_connector(sf_connector)
    engine.register_connector(tb_connector)
    engine.collect_lineage()
    graph = engine.get_unified_graph()
    assert len(graph["nodes"]) > 0
    os.unlink(config_file)


def test_query_lineage(config_file):
    engine = LineageEngine(config_file)
    sf_connector = SnowflakeConnector({"account": "test"})
    engine.register_connector(sf_connector)
    engine.collect_lineage()
    node = engine.query_lineage("sf.db.table1")
    assert node is not None
    assert node["id"] == "sf.db.table1"
    os.unlink(config_file)
