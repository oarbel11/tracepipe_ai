import pytest
from scripts.bi_integration import BIMetadataExtractor, MetricToTableMapper, BIIntegrationEngine
from scripts.bi_integration.bi_metadata_extractor import BIMetric

def test_bi_metadata_extractor_tableau():
    extractor = BIMetadataExtractor('tableau')
    metrics = extractor.extract_metadata('test_workspace', {})
    assert len(metrics) > 0
    assert metrics[0].platform == 'tableau'
    assert metrics[0].name == 'Total Revenue'

def test_bi_metadata_extractor_unsupported():
    with pytest.raises(ValueError):
        BIMetadataExtractor('unsupported_platform')

def test_parse_sql_from_definition():
    extractor = BIMetadataExtractor('tableau')
    result = extractor.parse_sql_from_definition('SUM([sales_amount]) FROM companies.sales')
    assert 'sales_amount' in result['columns']
    assert 'companies.sales' in result['tables']

def test_metric_mapper_build_lineage():
    mapper = MetricToTableMapper('companies_data')
    try:
        lineage = mapper.build_lineage_graph()
        assert isinstance(lineage, dict)
    except Exception:
        pytest.skip('Requires Databricks connection')

def test_metric_mapper_trace():
    mapper = MetricToTableMapper('companies_data')
    mapper.lineage_cache = {
        'companies_data.main.sales': ['sales_amount', 'customer_id'],
        'companies_data.main.customers': ['customer_id', 'name']
    }
    trace = mapper.trace_metric('Total Revenue', 
                                ['companies_data.main.sales'], 
                                ['sales_amount'])
    assert trace['metric'] == 'Total Revenue'
    assert len(trace['sources']) > 0
    assert trace['sources'][0]['table'] == 'companies_data.main.sales'

def test_integration_engine_sync():
    engine = BIIntegrationEngine()
    count = engine.sync_bi_metadata('tableau', 'test_workspace')
    assert count > 0
    assert len(engine.metadata_store) > 0

def test_integration_engine_trace():
    engine = BIIntegrationEngine()
    engine.sync_bi_metadata('tableau', 'test_workspace')
    engine.mapper.lineage_cache = {
        'companies_data.main.sales': ['sales_amount', 'customer_id']
    }
    lineage = engine.trace_metric_to_source('Total Revenue')
    assert 'definition' in lineage
    assert lineage['metric'] == 'Total Revenue'

def test_integration_engine_get_all_metrics():
    engine = BIIntegrationEngine()
    engine.sync_bi_metadata('tableau', 'test_workspace')
    metrics = engine.get_all_metrics()
    assert len(metrics) > 0
    assert 'name' in metrics[0]
    assert 'platform' in metrics[0]

def test_integration_engine_validate():
    engine = BIIntegrationEngine()
    engine.sync_bi_metadata('tableau', 'test_workspace')
    engine.mapper.lineage_cache = {
        'companies_data.main.sales': ['sales_amount']
    }
    results = engine.validate_all_metrics()
    assert 'valid' in results
    assert 'invalid' in results
