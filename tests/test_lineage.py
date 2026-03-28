"""Tests for unified cross-platform lineage feature."""

import pytest
from scripts.lineage.connector_registry import ConnectorRegistry
from scripts.lineage.lineage_graph import LineageGraph
from scripts.lineage.connectors.kafka_connector import KafkaConnector
from scripts.lineage.connectors.s3_connector import S3Connector
from scripts.lineage.connectors.powerbi_connector import PowerBIConnector
from scripts.lineage.connectors.tableau_connector import TableauConnector


def test_connector_registry():
    registry = ConnectorRegistry()
    kafka_config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
    kafka_connector = KafkaConnector(kafka_config)
    registry.register("kafka", kafka_connector)
    assert "kafka" in registry.list_connectors()
    assert registry.get("kafka") == kafka_connector
    assert registry.unregister("kafka") is True


def test_lineage_graph():
    graph = LineageGraph()
    graph.add_node("s3_bucket", "s3", {"bucket": "data-lake"})
    graph.add_node("kafka_topic", "kafka", {"topic": "events"})
    graph.add_node("powerbi_report", "powerbi", {"report": "sales"})
    graph.add_edge("s3_bucket", "kafka_topic")
    graph.add_edge("kafka_topic", "powerbi_report")
    assert len(graph.get_all_nodes()) == 3
    assert graph.get_downstream("s3_bucket") == ["kafka_topic"]
    assert graph.get_upstream("powerbi_report") == ["kafka_topic"]
    path = graph.build_lineage_path("s3_bucket", "powerbi_report")
    assert path == ["s3_bucket", "kafka_topic", "powerbi_report"]


def test_kafka_connector():
    config = {"bootstrap_servers": "localhost:9092", "topics": ["orders", "users"]}
    connector = KafkaConnector(config)
    assert connector.connector_type == "kafka"
    lineage = connector.extract_lineage()
    assert len(lineage) == 2
    assert lineage[0]["entity_type"] == "kafka_topic"


def test_s3_connector():
    config = {"bucket": "my-bucket", "prefixes": ["data/", "logs/"]}
    connector = S3Connector(config)
    assert connector.connector_type == "s3"
    lineage = connector.extract_lineage()
    assert len(lineage) == 2
    assert "s3://my-bucket/data/" in lineage[0]["entity_id"]


def test_powerbi_connector():
    config = {"workspace_id": "workspace1", "reports": ["sales", "marketing"]}
    connector = PowerBIConnector(config)
    assert connector.connector_type == "powerbi"
    lineage = connector.extract_lineage()
    assert len(lineage) == 2
    assert lineage[0]["entity_type"] == "powerbi_report"


def test_tableau_connector():
    config = {"server_url": "https://tableau.com", "workbooks": ["finance"]}
    connector = TableauConnector(config)
    assert connector.connector_type == "tableau"
    lineage = connector.extract_lineage()
    assert len(lineage) == 1
    assert lineage[0]["entity_type"] == "tableau_workbook"


def test_end_to_end_lineage():
    registry = ConnectorRegistry()
    graph = LineageGraph()
    s3_connector = S3Connector({"bucket": "raw", "prefixes": ["input/"]})
    kafka_connector = KafkaConnector({"topics": ["processed"]})
    powerbi_connector = PowerBIConnector({"workspace_id": "w1", "reports": ["dash"]})
    registry.register("s3", s3_connector)
    registry.register("kafka", kafka_connector)
    registry.register("powerbi", powerbi_connector)
    for lineage_item in s3_connector.extract_lineage():
        graph.add_node(lineage_item["entity_id"], lineage_item["entity_type"], lineage_item["metadata"])
    for lineage_item in kafka_connector.extract_lineage():
        graph.add_node(lineage_item["entity_id"], lineage_item["entity_type"], lineage_item["metadata"])
    for lineage_item in powerbi_connector.extract_lineage():
        graph.add_node(lineage_item["entity_id"], lineage_item["entity_type"], lineage_item["metadata"])
    assert len(graph.get_all_nodes()) == 3
