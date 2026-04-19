import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.spark_instrumentation import InstrumentedLineageExtractor, SparkASTAnalyzer
from scripts.cross_workspace_lineage import CrossWorkspaceLineage
from scripts.historical_lineage_store import HistoricalLineageStore
import tempfile
import json

def test_spark_ast_analysis():
    code = """
df = spark.sql("SELECT * FROM catalog.schema.table1")
df2 = df.withColumn("new_col", col("old_col"))
df3 = df2.join(other_df, "id")
"""
    extractor = InstrumentedLineageExtractor()
    lineage = extractor.extract_from_notebook(code)
    assert 'nodes' in lineage
    assert 'edges' in lineage
    assert len(lineage['edges']) > 0

def test_udf_extraction():
    code = """my_udf = udf(lambda x: x.upper())
df = df.withColumn('new', my_udf(col('old')))"""
    extractor = InstrumentedLineageExtractor()
    lineage = extractor.extract_from_notebook(code)
    assert len(lineage['udf_calls']) > 0

def test_historical_store():
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as tmp:
        store = HistoricalLineageStore(tmp.name)
        lineage_data = {'tables': ['t1', 't2'], 'edges': []}
        snapshot_id = store.snapshot_lineage('test_ws', lineage_data)
        assert snapshot_id.startswith('test_ws')
        snapshots = store.get_snapshots('test_ws')
        assert len(snapshots) == 1
        store.close()
        os.unlink(tmp.name)

def test_table_lineage_history():
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as tmp:
        store = HistoricalLineageStore(tmp.name)
        store.store_table_lineage('cat.sch.table1', ['upstream1'], ['downstream1'], 'prod')
        history = store.query_historical_lineage('cat.sch.table1', '2020-01-01', '2030-01-01')
        assert len(history) == 1
        assert history[0]['table'] == 'cat.sch.table1'
        store.close()
        os.unlink(tmp.name)

def test_cross_workspace_mock():
    cwl = CrossWorkspaceLineage()
    assert hasattr(cwl, 'workspaces')
    assert hasattr(cwl, 'lineage_cache')
