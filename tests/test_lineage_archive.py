import pytest
from datetime import datetime, timedelta
import os
import tempfile
from tracepipe_ai.lineage_archive import LineageArchive


@pytest.fixture
def archive():
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    arc = LineageArchive(db_path)
    yield arc
    arc.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


def test_archive_lineage(archive):
    timestamp = datetime.now()
    result = archive.archive_lineage(
        event_id="evt_001",
        timestamp=timestamp,
        source_table="catalog.schema.source",
        target_table="catalog.schema.target",
        operation_type="INSERT",
        user_name="test_user",
        metadata={"job_id": "123"}
    )
    assert result is True


def test_query_lineage(archive):
    now = datetime.now()
    archive.archive_lineage(
        "evt_001", now, "src1", "tgt1", "INSERT", "user1", {"k": "v"}
    )
    archive.archive_lineage(
        "evt_002", now + timedelta(hours=1), "src2", "tgt2",
        "UPDATE", "user2", {}
    )
    results = archive.query_lineage(now - timedelta(days=1), now + timedelta(days=1))
    assert len(results) == 2


def test_query_lineage_with_table_filter(archive):
    now = datetime.now()
    archive.archive_lineage("evt_001", now, "src1", "tgt1", "INSERT", "user1")
    archive.archive_lineage("evt_002", now, "src2", "tgt2", "INSERT", "user2")
    results = archive.query_lineage(
        now - timedelta(days=1), now + timedelta(days=1), "src1"
    )
    assert len(results) == 1
    assert results[0]["source_table"] == "src1"


def test_get_statistics(archive):
    now = datetime.now()
    archive.archive_lineage("evt_001", now, "src", "tgt", "INSERT", "user")
    stats = archive.get_statistics()
    assert stats["total_events"] == 1
    assert stats["oldest_event"] is not None
