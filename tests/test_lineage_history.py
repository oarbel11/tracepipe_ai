import pytest
import pandas as pd
from datetime import datetime, timedelta
import os
from scripts.lineage_history import LineageHistoryStore


@pytest.fixture
def temp_store(tmp_path):
    db_path = tmp_path / "test_lineage.duckdb"
    store = LineageHistoryStore(str(db_path))
    yield store
    store.close()
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def sample_lineage():
    return pd.DataFrame({
        'source_table': ['catalog.schema.table1', 'catalog.schema.table2'],
        'target_table': ['catalog.schema.table3', 'catalog.schema.table3'],
        'source_column': ['col1', 'col2'],
        'target_column': ['col_a', 'col_b'],
        'lineage_type': ['table', 'column']
    })


def test_export_lineage(temp_store, sample_lineage):
    count = temp_store.export_lineage(sample_lineage)
    assert count == 2


def test_time_travel_all_records(temp_store, sample_lineage):
    temp_store.export_lineage(sample_lineage)
    future_date = datetime.now() + timedelta(days=1)
    result = temp_store.time_travel(future_date)
    assert len(result) == 2


def test_time_travel_with_filter(temp_store, sample_lineage):
    temp_store.export_lineage(sample_lineage)
    future_date = datetime.now() + timedelta(days=1)
    result = temp_store.time_travel(future_date, 'catalog.schema.table1')
    assert len(result) >= 1


def test_time_travel_past_date(temp_store, sample_lineage):
    past_date = datetime.now() - timedelta(days=1)
    temp_store.export_lineage(sample_lineage)
    result = temp_store.time_travel(past_date)
    assert len(result) == 0


def test_lineage_evolution(temp_store, sample_lineage):
    temp_store.export_lineage(sample_lineage)
    start = datetime.now() - timedelta(days=1)
    end = datetime.now() + timedelta(days=1)
    evolution = temp_store.get_lineage_evolution('catalog.schema.table3', start, end)
    assert len(evolution) == 2
    assert evolution[0]['target'] == 'catalog.schema.table3'


def test_multiple_snapshots(temp_store, sample_lineage):
    temp_store.export_lineage(sample_lineage)
    temp_store.export_lineage(sample_lineage)
    result = temp_store.time_travel(datetime.now() + timedelta(days=1))
    assert len(result) == 4
