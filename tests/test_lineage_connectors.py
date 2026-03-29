import pytest
from datetime import datetime
from connectors.base import ConnectorConfig, LineageEdge, BaseConnector
from connectors.external_sources import KafkaConnector, S3Connector
from connectors.lineage_engine import LineageInferenceEngine


def test_connector_config():
    config = ConnectorConfig(
        connector_type='kafka',
        host='localhost',
        port=9092
    )
    assert config.connector_type == 'kafka'
    assert config.host == 'localhost'


def test_lineage_edge_creation():
    edge = LineageEdge(
        source_system='kafka',
        source_asset='user_events',
        target_system='databricks',
        target_asset='bronze.events',
        operation_type='stream',
        timestamp=datetime.now(),
        metadata={}
    )
    assert edge.source_system == 'kafka'
    assert edge.operation_type == 'stream'


def test_kafka_connector_inference():
    config = ConnectorConfig(connector_type='kafka')
    connector = KafkaConnector(config)
    
    catalog = {
        'bronze.kafka_events': {
            'properties': {'kafka.topic': 'user_events'},
            'source_format': 'kafka'
        }
    }
    
    edges = connector.infer_relationships(catalog)
    assert len(edges) > 0
    assert edges[0].source_system == 'kafka'


def test_s3_connector_inference():
    config = ConnectorConfig(connector_type='s3')
    connector = S3Connector(config)
    
    catalog = {
        'bronze.raw_data': {
            'location': 's3://my-bucket/raw/data/',
            'format': 'parquet'
        }
    }
    
    edges = connector.infer_relationships(catalog)
    assert len(edges) == 1
    assert edges[0].source_system == 's3'
    assert 's3://' in edges[0].source_asset


def test_lineage_engine():
    engine = LineageInferenceEngine()
    
    kafka_config = ConnectorConfig(connector_type='kafka')
    kafka_connector = KafkaConnector(kafka_config)
    engine.register_connector(kafka_connector)
    
    catalog = {
        'bronze.events': {'properties': {'kafka.topic': 'events'}},
        'silver.processed_events': {}
    }
    
    graph = engine.build_lineage_graph(catalog)
    assert graph is not None
    assert len(graph.nodes) >= 2


def test_lineage_export():
    engine = LineageInferenceEngine()
    s3_connector = S3Connector(ConnectorConfig(connector_type='s3'))
    engine.register_connector(s3_connector)
    
    catalog = {'bronze.data': {'location': 's3://bucket/path/'}}
    engine.build_lineage_graph(catalog)
    
    export = engine.export_lineage()
    assert 'nodes' in export
    assert 'edges' in export
