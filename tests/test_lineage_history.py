import pytest
import os
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil

from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def temp_db():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_lineage.db")
    yield db_path
    shutil.rmtree(temp_dir)


def test_init_storage(temp_db):
    storage = LineageHistoryStorage(temp_db)
    assert os.path.exists(temp_db)


def test_store_snapshot(temp_db):
    storage = LineageHistoryStorage(temp_db)
    snapshot_id = storage.store_snapshot(
        table_name="catalog.schema.table1",
        upstream=["catalog.schema.source1", "catalog.schema.source2"],
        downstream=["catalog.schema.target1"],
        metadata={"query": "SELECT * FROM source1", "user": "admin"}
    )
    assert snapshot_id is not None
    assert "catalog.schema.table1" in snapshot_id


def test_query_lineage_by_table(temp_db):
    storage = LineageHistoryStorage(temp_db)
    storage.store_snapshot(
        "catalog.schema.table1",
        ["source1"],
        ["target1"],
        {"version": "1"}
    )
    storage.store_snapshot(
        "catalog.schema.table2",
        ["source2"],
        ["target2"],
        {"version": "1"}
    )
    
    results = storage.query_lineage(table_name="catalog.schema.table1")
    assert len(results) == 1
    assert results[0]["table_name"] == "catalog.schema.table1"
    assert results[0]["upstream_tables"] == ["source1"]


def test_query_lineage_by_time(temp_db):
    storage = LineageHistoryStorage(temp_db)
    now = datetime.utcnow()
    
    storage.store_snapshot("table1", ["s1"], ["t1"], {})
    
    results = storage.query_lineage(
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(hours=1)
    )
    assert len(results) >= 1


def test_query_all_lineage(temp_db):
    storage = LineageHistoryStorage(temp_db)
    storage.store_snapshot("table1", ["s1"], ["t1"], {})
    storage.store_snapshot("table2", ["s2"], ["t2"], {})
    
    results = storage.query_lineage()
    assert len(results) == 2
