import pytest
from scripts.lineage import LineageGraphBuilder, ConnectorRegistry, LineageStitcher

def test_lineage_graph_builder():
    builder = LineageGraphBuilder()
    builder.add_node('db1.table1', 'table', 'databricks')
    builder.add_node('db1.table2', 'table', 'databricks')
    builder.add_edge('db1.table1', 'db1.table2')
    
    assert len(builder.nodes) == 2
    assert len(builder.edges) == 1
    assert builder.get_downstream('db1.table1') == ['db1.table2']
    assert builder.get_upstream('db1.table2') == ['db1.table1']

def test_connector_registry():
    registry = ConnectorRegistry()
    
    def mock_connector(config):
        return {'nodes': [], 'edges': []}
    
    registry.register_connector('test_connector', mock_connector)
    assert 'test_connector' in registry.list_connectors()
    assert registry.get_connector('test_connector') is not None

def test_lineage_stitcher():
    builder = LineageGraphBuilder()
    registry = ConnectorRegistry()
    
    def unity_catalog_connector(config):
        return {
            'nodes': [
                {'id': 'uc.catalog.table1', 'type': 'table'},
                {'id': 'uc.catalog.table2', 'type': 'table'}
            ],
            'edges': [{'source': 'uc.catalog.table1', 'target': 'uc.catalog.table2'}]
        }
    
    registry.register_connector('unity_catalog', unity_catalog_connector)
    stitcher = LineageStitcher(builder, registry)
    
    source_configs = [{'connector': 'unity_catalog', 'config': {}}]
    summary = stitcher.stitch_lineage(source_configs)
    
    assert summary['node_count'] == 2
    assert summary['edge_count'] == 1
    assert 'unity_catalog' in summary['platforms']

def test_cross_platform_lineage():
    builder = LineageGraphBuilder()
    registry = ConnectorRegistry()
    
    def db_connector(config):
        return {'nodes': [{'id': 'mysql.db.users', 'type': 'table'}], 'edges': []}
    
    def uc_connector(config):
        return {
            'nodes': [{'id': 'uc.raw.users', 'type': 'table'}],
            'edges': [{'source': 'mysql.db.users', 'target': 'uc.raw.users'}]
        }
    
    registry.register_connector('mysql', db_connector)
    registry.register_connector('unity_catalog', uc_connector)
    stitcher = LineageStitcher(builder, registry)
    
    configs = [
        {'connector': 'mysql', 'config': {}},
        {'connector': 'unity_catalog', 'config': {}}
    ]
    stitcher.stitch_lineage(configs)
    platform_summary = stitcher.get_platform_summary()
    
    assert len(platform_summary) == 2
    assert 'mysql' in platform_summary
    assert 'unity_catalog' in platform_summary
