import pytest
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from tracepipe_ai.lineage_history import LineageHistoryStorage


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    fd, path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def storage(temp_db):
    """Create a LineageHistoryStorage instance."""
    return LineageHistoryStorage(db_path=temp_db)


def test_store_lineage(storage):
    """Test storing lineage records."""
    lineage_id = storage.store_lineage(
        source="table_a",
        target="table_b",
        metadata={"operation": "transform"}
    )
    assert lineage_id > 0


def test_query_all_lineage(storage):
    """Test querying all lineage records."""
    storage.store_lineage("table_a", "table_b", {"op": "join"})
    storage.store_lineage("table_b", "table_c", {"op": "filter"})
    
    results = storage.query_lineage()
    assert len(results) == 2
    assert results[0]["source_table"] in ["table_a", "table_b"]


def test_query_by_table(storage):
    """Test querying lineage by table name."""
    storage.store_lineage("table_a", "table_b", {})
    storage.store_lineage("table_c", "table_d", {})
    
    results = storage.query_lineage(table="table_a")
    assert len(results) == 1
    assert results[0]["source_table"] == "table_a"


def test_query_by_date_range(storage):
    """Test querying lineage by date range."""
    now = datetime.now()
    storage.store_lineage("table_a", "table_b", {})
    
    results = storage.query_lineage(
        start_date=now - timedelta(days=1),
        end_date=now + timedelta(days=1)
    )
    assert len(results) == 1


def test_metadata_persistence(storage):
    """Test that metadata is correctly stored and retrieved."""
    metadata = {"user": "test", "pipeline": "etl_001"}
    storage.store_lineage("src", "dst", metadata)
    
    results = storage.query_lineage()
    assert results[0]["metadata"] == metadata
