import pytest
from datetime import datetime, timedelta
import os
import tempfile
from tracepipe_ai.lineage_archive import LineageArchive


@pytest.fixture
def archive():
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    
    arch = LineageArchive(db_path)
    yield arch
    arch.close()
    
    try:
        os.unlink(db_path)
    except:
        pass


def test_archive_initialization(archive):
    assert archive is not None
    assert archive.conn is not None


def test_archive_event(archive):
    event = {
        "event_id": "evt_001",
        "event_type": "table_read",
        "source_table": "catalog.schema.table_a",
        "target_table": "catalog.schema.table_b",
        "timestamp": datetime.now(),
        "metadata": {"user": "test_user", "query_id": "q123"}
    }
    archive.archive_event(event)
    
    results = archive.query_by_table("catalog.schema.table_a")
    assert len(results) == 1
    assert results[0]["event_id"] == "evt_001"
    assert results[0]["event_type"] == "table_read"


def test_query_by_date_range(archive):
    now = datetime.now()
    past = now - timedelta(days=1)
    future = now + timedelta(days=1)
    
    event1 = {
        "event_id": "evt_001",
        "event_type": "table_read",
        "source_table": "table_a",
        "target_table": "table_b",
        "timestamp": now,
        "metadata": {}
    }
    archive.archive_event(event1)
    
    results = archive.query_by_date_range(past, future)
    assert len(results) >= 1
    assert any(r["event_id"] == "evt_001" for r in results)


def test_query_by_table(archive):
    events = [
        {
            "event_id": f"evt_{i}",
            "event_type": "table_read",
            "source_table": "src_table",
            "target_table": f"target_{i}",
            "timestamp": datetime.now(),
            "metadata": {}
        }
        for i in range(3)
    ]
    
    for event in events:
        archive.archive_event(event)
    
    results = archive.query_by_table("src_table")
    assert len(results) == 3
