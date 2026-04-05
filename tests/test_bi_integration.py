import pytest
from scripts.bi_integration import BIMetadataExtractor, MetricToTableMapper, BIIntegrationEngine

def test_metadata_extractor_powerbi():
    extractor = BIMetadataExtractor('powerbi', {})
    dashboards = extractor.extract_dashboards()
    assert len(dashboards) > 0
    assert dashboards[0]['id'] == 'dash_1'
    assert dashboards[0]['name'] == 'Sales Dashboard'

def test_metadata_extractor_tableau():
    extractor = BIMetadataExtractor('tableau', {})
    dashboards = extractor.extract_dashboards()
    assert len(dashboards) > 0
    assert dashboards[0]['id'] == 'dash_2'

def test_metadata_extractor_looker():
    extractor = BIMetadataExtractor('looker', {})
    dashboards = extractor.extract_dashboards()
    assert len(dashboards) > 0
    assert dashboards[0]['id'] == 'dash_3'

def test_extract_metrics():
    extractor = BIMetadataExtractor('powerbi', {})
    metrics = extractor.extract_metrics('dash_1')
    assert len(metrics) > 0
    assert metrics[0]['name'] == 'Total Revenue'

def test_metric_mapper_extract_tables():
    lineage_data = {'sales': {'upstream': [], 'downstream': []}}
    mapper = MetricToTableMapper(lineage_data)
    tables = mapper._extract_tables_from_query('SELECT SUM(amount) FROM sales')
    assert 'sales' in tables

def test_metric_mapper_map_to_tables():
    lineage_data = {'sales': {'upstream': [], 'downstream': []}}
    mapper = MetricToTableMapper(lineage_data)
    metric = {'name': 'Revenue', 'query': 'SELECT SUM(amount) FROM sales'}
    mappings = mapper.map_metric_to_tables(metric)
    assert len(mappings) > 0
    assert mappings[0]['table'] == 'sales'

def test_bi_integration_engine_sync():
    lineage_data = {}
    engine = BIIntegrationEngine('powerbi', {}, lineage_data)
    results = engine.sync_metadata()
    assert 'dashboards' in results
    assert results['total_metrics'] > 0

def test_bi_integration_engine_query_lineage():
    lineage_data = {}
    engine = BIIntegrationEngine('powerbi', {}, lineage_data)
    lineage = engine.query_metric_lineage('Total Revenue')
    assert lineage['metric'] == 'Total Revenue'
    assert 'mappings' in lineage
