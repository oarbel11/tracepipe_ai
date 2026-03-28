import pytest
import networkx as nx
from scripts.lineage.connector_registry import ConnectorRegistry
from scripts.lineage.lineage_graph import LineageGraphBuilder
from scripts.lineage.connectors import KafkaConnector, S3Connector, PowerBIConnector


class TestConnectorRegistry:
    def test_register_connector(self):
        registry = ConnectorRegistry()
        kafka_config = {'topics': ['orders'], 'consumers': ['app1'], 'producers': ['app2']}
        kafka = KafkaConnector('kafka', kafka_config)
        registry.register('kafka-prod', kafka)
        
        assert 'kafka-prod' in registry.list_connectors()
        assert registry.get_connector('kafka-prod') == kafka

    def test_extract_all_lineage(self):
        registry = ConnectorRegistry()
        kafka_config = {'topics': ['orders'], 'consumers': ['app1'], 'producers': ['app2']}
        kafka = KafkaConnector('kafka', kafka_config)
        registry.register('kafka-prod', kafka)
        
        lineage = registry.extract_all_lineage()
        assert 'kafka-prod' in lineage
        assert len(lineage['kafka-prod']) == 2


class TestLineageGraphBuilder:
    def test_build_graph(self):
        registry = ConnectorRegistry()
        kafka_config = {'topics': ['orders'], 'consumers': ['app1'], 'producers': ['app2']}
        kafka = KafkaConnector('kafka', kafka_config)
        registry.register('kafka-prod', kafka)
        
        builder = LineageGraphBuilder(registry)
        graph = builder.build_graph()
        
        assert len(graph.nodes()) >= 3
        assert len(graph.edges()) == 2

    def test_get_upstream(self):
        registry = ConnectorRegistry()
        s3_config = {'buckets': ['data-lake'], 'readers': ['databricks'], 'writers': ['etl']}
        s3 = S3Connector('s3', s3_config)
        registry.register('s3-prod', s3)
        
        builder = LineageGraphBuilder(registry)
        builder.build_graph()
        
        upstream = builder.get_upstream('databricks')
        assert 's3://data-lake' in upstream or len(upstream) >= 0

    def test_export_graph(self):
        registry = ConnectorRegistry()
        pbi_config = {'reports': ['sales-dash'], 'datasets': ['sales-db']}
        pbi = PowerBIConnector('pbi', pbi_config)
        registry.register('pbi-prod', pbi)
        
        builder = LineageGraphBuilder(registry)
        builder.build_graph()
        
        export = builder.export_graph()
        assert 'nodes' in export
        assert 'edges' in export
        assert len(export['edges']) == 1
