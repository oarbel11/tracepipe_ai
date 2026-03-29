import pytest
import os
import tempfile
from datetime import datetime
from pathlib import Path

from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.unlink(db_path)


def test_lineage_history_init(temp_db):
    """Test LineageHistoryStorage initialization."""
    storage = LineageHistoryStorage(db_path=temp_db)
    assert os.path.exists(temp_db)


def test_store_and_query_snapshot(temp_db):
    """Test storing and querying lineage snapshots."""
    storage = LineageHistoryStorage(db_path=temp_db)
    lineage_data = [
        {
            "source_table": "catalog.schema.table_a",
            "target_table": "catalog.schema.table_b",
            "lineage_type": "table",
            "metadata": {"operation": "INSERT", "user": "test_user"}
        },
        {
            "source_table": "catalog.schema.table_b",
            "target_table": "catalog.schema.table_c",
            "lineage_type": "table",
            "metadata": {"operation": "MERGE"}
        }
    ]
    storage.store_snapshot(lineage_data)
    results = storage.query_historical_lineage("catalog.schema.table_b")
    assert len(results) == 2
    assert results[0]["source_table"] in ["catalog.schema.table_a", "catalog.schema.table_b"]
    assert results[0]["metadata"]["operation"] in ["INSERT", "MERGE"]


def test_get_snapshot_dates(temp_db):
    """Test retrieving snapshot dates."""
    storage = LineageHistoryStorage(db_path=temp_db)
    lineage_data = [{"source_table": "t1", "target_table": "t2", "lineage_type": "table"}]
    storage.store_snapshot(lineage_data)
    dates = storage.get_snapshot_dates()
    assert len(dates) >= 1
    assert isinstance(dates[0], str)


def test_query_with_date_range(temp_db):
    """Test querying with date filters."""
    storage = LineageHistoryStorage(db_path=temp_db)
    lineage_data = [{"source_table": "t1", "target_table": "t2", "lineage_type": "table"}]
    storage.store_snapshot(lineage_data)
    start = "2020-01-01"
    end = "2030-12-31"
    results = storage.query_historical_lineage("t1", start_date=start, end_date=end)
    assert len(results) == 1
