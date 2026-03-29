import pytest
from tracepipe_ai.cross_system_lineage import (
    LineageIntegrator, LineageStitcher, ConnectorConfig
)


class MockConnector:
    def get_lineage(self, filters):
        return [
            {'source': 'table1', 'target': 'table2',
             'source_metadata': {'type': 'table'},
             'target_metadata': {'type': 'table'}}
        ]


def test_connector_config():
    config = ConnectorConfig()
    config.add_connector('test_bi', 'tableau',
                         {'host': 'localhost', 'port': 8080})
    assert 'test_bi' in config.list_connectors()
    assert config.get_connector('test_bi')['type'] == 'tableau'
    assert config.remove_connector('test_bi') is True


def test_lineage_integrator():
    integrator = LineageIntegrator()
    connector = MockConnector()
    integrator.register_connector('mock', connector)
    assert 'mock' in integrator.list_connectors()
    lineage = integrator.fetch_lineage('mock')
    assert len(lineage) == 1
    assert lineage[0]['source'] == 'table1'
    cached = integrator.get_cached_lineage('mock')
    assert cached == lineage
    integrator.clear_cache('mock')
    assert integrator.get_cached_lineage('mock') is None


def test_lineage_stitcher():
    stitcher = LineageStitcher()
    uc_lineage = [
        {'source': 'catalog.schema.table1', 'target': 'catalog.schema.table2',
         'source_metadata': {}, 'target_metadata': {}}
    ]
    stitcher.add_unity_catalog_lineage(uc_lineage)
    external_lineage = [
        {'source': 'dashboard1', 'target': 'dashboard2',
         'source_metadata': {}, 'target_metadata': {}}
    ]
    stitcher.add_external_lineage('tableau', external_lineage)
    result = stitcher.get_complete_lineage()
    assert len(result['nodes']) == 4
    assert len(result['edges']) == 2


def test_lineage_stitcher_matching():
    stitcher = LineageStitcher()
    uc_lineage = [
        {'source': 'catalog.schema.sales', 'target': 'catalog.schema.report',
         'source_metadata': {}, 'target_metadata': {}}
    ]
    stitcher.add_unity_catalog_lineage(uc_lineage)
    matching_rules = [{'source_pattern': 'sales', 'target_pattern': 'report'}]
    result = stitcher.stitch_lineage(matching_rules)
    assert len(result['nodes']) >= 2


def test_lineage_navigation():
    stitcher = LineageStitcher()
    stitcher.add_unity_catalog_lineage([
        {'source': 'A', 'target': 'B', 'source_metadata': {},
         'target_metadata': {}},
        {'source': 'B', 'target': 'C', 'source_metadata': {},
         'target_metadata': {}}
    ])
    downstream = stitcher.get_downstream('A')
    assert 'B' in downstream
    upstream = stitcher.get_upstream('C')
    assert 'B' in upstream
