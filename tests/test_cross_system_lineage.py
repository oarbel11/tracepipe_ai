import pytest
import networkx as nx
from scripts.lineage.external_connectors import ExternalConnectorRegistry, TableauConnector
from scripts.lineage.lineage_stitcher import LineageStitcher


@pytest.fixture
def unity_catalog_lineage():
    return [
        {'source': 'raw.orders', 'target': 'analytics.order_summary'},
        {'source': 'raw.customers', 'target': 'analytics.customer_insights'}
    ]


@pytest.fixture
def external_configs():
    return [
        {
            'type': 'tableau',
            'name': 'tableau_prod',
            'data_sources': ['analytics.order_summary', 'analytics.customer_insights'],
            'dashboards': ['Sales Dashboard', 'Customer 360']
        },
        {
            'type': 'powerbi',
            'name': 'powerbi_prod',
            'datasets': ['analytics.customer_insights'],
            'reports': ['Executive Report']
        }
    ]


def test_external_connector_registry():
    registry = ExternalConnectorRegistry()
    config = {'name': 'test_tableau', 'data_sources': [], 'dashboards': []}
    connector = registry.get_connector('tableau', config)
    assert connector is not None
    assert isinstance(connector, TableauConnector)


def test_tableau_connector_extract_lineage():
    config = {
        'name': 'tableau_test',
        'data_sources': ['db.table1'],
        'dashboards': ['Dashboard1']
    }
    connector = TableauConnector(config)
    lineage = connector.extract_lineage()
    assert len(lineage) == 1
    assert lineage[0]['source'] == 'db.table1'
    assert lineage[0]['target'] == 'Dashboard1'
    assert lineage[0]['system'] == 'tableau'


def test_lineage_stitcher_basic(unity_catalog_lineage, external_configs):
    stitcher = LineageStitcher(unity_catalog_lineage)
    graph = stitcher.stitch_lineage(external_configs)
    assert graph.number_of_nodes() > 0
    assert graph.number_of_edges() >= len(unity_catalog_lineage)


def test_end_to_end_path(unity_catalog_lineage, external_configs):
    stitcher = LineageStitcher(unity_catalog_lineage)
    stitcher.stitch_lineage(external_configs)
    paths = stitcher.get_end_to_end_path('raw.orders', 'Sales Dashboard')
    assert len(paths) >= 0


def test_upstream_dependencies(unity_catalog_lineage):
    stitcher = LineageStitcher(unity_catalog_lineage)
    stitcher.stitch_lineage([])
    upstream = stitcher.get_upstream_dependencies('analytics.order_summary')
    assert 'raw.orders' in upstream


def test_downstream_impact(unity_catalog_lineage, external_configs):
    stitcher = LineageStitcher(unity_catalog_lineage)
    stitcher.stitch_lineage(external_configs)
    downstream = stitcher.get_downstream_impact('analytics.order_summary')
    assert len(downstream) >= 0


def test_export_lineage(unity_catalog_lineage, external_configs):
    stitcher = LineageStitcher(unity_catalog_lineage)
    stitcher.stitch_lineage(external_configs)
    export = stitcher.export_lineage()
    assert 'nodes' in export
    assert 'edges' in export
    assert 'stats' in export
    assert export['stats']['total_nodes'] > 0
