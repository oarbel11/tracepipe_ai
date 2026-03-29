import pytest
import json
import tempfile
import os
from datetime import datetime, timedelta
from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sample_lineage():
    return [
        {
            'source_table': 'catalog.schema.source_table',
            'target_table': 'catalog.schema.target_table',
            'source_type': 'TABLE',
            'target_type': 'TABLE',
            'metadata': {'operation': 'INSERT', 'timestamp': '2024-01-01'}
        },
        {
            'source_table': 'catalog.schema.another_source',
            'target_table': 'catalog.schema.target_table',
            'source_type': 'TABLE',
            'target_type': 'TABLE',
            'metadata': {'operation': 'MERGE', 'timestamp': '2024-01-02'}
        }
    ]


def test_storage_initialization(temp_db):
    storage = LineageHistoryStorage(temp_db)
    assert os.path.exists(temp_db)


def test_store_lineage(temp_db, sample_lineage):
    storage = LineageHistoryStorage(temp_db)
    snapshot_id = storage.store_lineage(sample_lineage)
    assert snapshot_id is not None
    assert isinstance(snapshot_id, str)


def test_query_all_lineage(temp_db, sample_lineage):
    storage = LineageHistoryStorage(temp_db)
    storage.store_lineage(sample_lineage)
    results = storage.query_lineage()
    assert len(results) == 2
    assert results[0]['source_table'] == 'catalog.schema.source_table'


def test_query_by_table_name(temp_db, sample_lineage):
    storage = LineageHistoryStorage(temp_db)
    storage.store_lineage(sample_lineage)
    results = storage.query_lineage(table_name='catalog.schema.target_table')
    assert len(results) == 2


def test_query_by_date_range(temp_db, sample_lineage):
    storage = LineageHistoryStorage(temp_db)
    storage.store_lineage(sample_lineage)
    
    yesterday = (datetime.now() - timedelta(days=1)).isoformat()
    tomorrow = (datetime.now() + timedelta(days=1)).isoformat()
    
    results = storage.query_lineage(start_date=yesterday, end_date=tomorrow)
    assert len(results) == 2


def test_time_travel_query(temp_db, sample_lineage):
    storage = LineageHistoryStorage(temp_db)
    snapshot1 = storage.store_lineage(sample_lineage[:1])
    snapshot2 = storage.store_lineage(sample_lineage[1:])
    
    all_results = storage.query_lineage()
    assert len(all_results) == 2
