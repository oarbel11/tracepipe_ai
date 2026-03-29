"""Tests for cross-system lineage connectors."""

from connectors.base import ConnectorConfig, LineageEdge, BaseConnector
from connectors.external_sources import KafkaConnector, S3Connector, ExternalDatabaseConnector
from connectors.lineage_engine import LineageInferenceEngine
from scripts.lineage_mapper import LineageMapper, create_connector


def test_connector_config():
    """Test connector configuration."""
    config = ConnectorConfig(
        name='test',
        connector_type='kafka',
        credentials={'user': 'test'},
        connection_params={'host': 'localhost'}
    )
    assert config.name == 'test'
    assert config.connector_type == 'kafka'


def test_lineage_edge():
    """Test lineage edge creation."""
    edge = LineageEdge(
        source_system='kafka',
        source_asset='topic1',
        target_system='databricks',
        target_asset='table1'
    )
    assert edge.source_system == 'kafka'
    assert edge.target_asset == 'table1'


def test_kafka_connector():
    """Test Kafka connector."""
    config = ConnectorConfig('kafka', 'kafka', {}, {})
    connector = KafkaConnector(config)
    assert connector.connect()
    assert connector.is_connected()
    connector.disconnect()
    assert not connector.is_connected()


def test_s3_connector():
    """Test S3 connector."""
    config = ConnectorConfig('s3', 's3', {}, {})
    connector = S3Connector(config)
    assert connector.connect()
    assert isinstance(connector.discover_assets(), list)


def test_database_connector():
    """Test database connector."""
    config = ConnectorConfig('db', 'database', {}, {})
    connector = ExternalDatabaseConnector(config)
    assert connector.connect()
    assert isinstance(connector.extract_lineage(), list)


def test_lineage_engine():
    """Test lineage inference engine."""
    engine = LineageInferenceEngine()
    edge = LineageEdge('s3', 'bucket/file', 'databricks', 'table1')
    engine.add_edge(edge)
    assert len(engine.get_all_edges()) == 1
    downstream = engine.get_downstream('s3', 'bucket/file')
    assert len(downstream) == 1
    assert downstream[0] == ('databricks', 'table1')


def test_lineage_mapper():
    """Test lineage mapper orchestration."""
    mapper = LineageMapper()
    config = ConnectorConfig('test', 'kafka', {}, {})
    connector = KafkaConnector(config)
    mapper.add_connector(connector)
    engine = mapper.map_lineage()
    assert isinstance(engine, LineageInferenceEngine)


def test_connector_factory():
    """Test connector factory."""
    config = ConnectorConfig('test', 'kafka', {}, {})
    connector = create_connector('kafka', config)
    assert isinstance(connector, KafkaConnector)
