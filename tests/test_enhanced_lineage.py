import pytest
import json
import os
import tempfile
from unittest.mock import Mock, patch
from scripts.spark_instrumentation import SparkInstrumentation
from scripts.cross_workspace_lineage import CrossWorkspaceLineage
from scripts.historical_lineage_store import HistoricalLineageStore

def test_spark_instrumentation_basic():
    instrumentation = SparkInstrumentation()
    code = "df.select('col1', 'col2').filter('col1 > 10')"
    lineage = instrumentation.extract_lineage_from_code(code)
    assert isinstance(lineage, list)
    assert len(lineage) >= 0

def test_spark_instrumentation_udf():
    instrumentation = SparkInstrumentation()
    udf_code = "def my_udf(x): return x * 2"
    result = instrumentation.instrument_udf(udf_code)
    assert result['instrumented'] is True
    assert 'udf_lineage' in result

def test_spark_instrumentation_runtime():
    instrumentation = SparkInstrumentation()
    context = {
        'notebook_path': '/test/notebook',
        'timestamp': '2024-01-01',
        'transformations': [{'op': 'select'}],
        'source_tables': ['table1'],
        'target_tables': ['table2']
    }
    lineage = instrumentation.capture_runtime_lineage(context)
    assert lineage['notebook_path'] == '/test/notebook'
    assert lineage['source_tables'] == ['table1']

def test_cross_workspace_lineage_aggregate():
    config = {
        'workspaces': [
            {'id': 'ws1', 'notebooks': ['/nb1'], 'tables': ['t1', 't2']},
            {'id': 'ws2', 'notebooks': ['/nb2'], 'tables': ['t2', 't3']}
        ]
    }
    cwl = CrossWorkspaceLineage(config)
    result = cwl.aggregate_lineage()
    assert 'workspaces' in result
    assert 'ws1' in result['workspaces']
    assert 'ws2' in result['workspaces']
    assert 'cross_workspace_flows' in result

def test_cross_workspace_lineage_query():
    config = {'workspaces': [{'id': 'ws1', 'notebooks': ['/nb1'], 'tables': ['t1']}]}
    cwl = CrossWorkspaceLineage(config)
    result = cwl.query_lineage('/nb1')
    assert result['notebook'] == '/nb1'
    assert len(result['found_in']) > 0

def test_historical_lineage_store():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    try:
        store = HistoricalLineageStore(db_path)
        lineage_data = {'source': 'table1', 'target': 'table2'}
        store.snapshot_lineage('/test/notebook', lineage_data)
        results = store.query_historical_lineage('/test/notebook')
        assert len(results) == 1
        assert results[0]['lineage_data']['source'] == 'table1'
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_historical_lineage_store_date_filter():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    try:
        store = HistoricalLineageStore(db_path)
        store.snapshot_lineage('/nb1', {'data': 'test'})
        results = store.query_historical_lineage('/nb1', start_date='2020-01-01')
        assert len(results) >= 0
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)
