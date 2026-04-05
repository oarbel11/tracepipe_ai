import pytest
from datetime import datetime, timedelta
import tempfile
import os
from tracepipe_ai.lineage_archive import LineageArchive


@pytest.fixture
def archive():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb") as f:
        db_path = f.name
    arch = LineageArchive(db_path)
    yield arch
    arch.close()
    os.unlink(db_path)


def test_archive_lineage(archive):
    lineage_data = {
        "id": "test_event_1",
        "event_time": datetime.now(),
        "table_name": "catalog.schema.table1",
        "upstream_tables": ["catalog.schema.source1"],
        "downstream_tables": ["catalog.schema.target1"],
        "operation_type": "INSERT",
        "user_name": "test_user",
        "metadata": {"job_id": "123"}
    }
    event_id = archive.archive_lineage(lineage_data)
    assert event_id == "test_event_1"


def test_query_lineage(archive):
    now = datetime.now()
    lineage_data = {
        "event_time": now,
        "table_name": "catalog.schema.table1",
        "operation_type": "INSERT",
        "user_name": "test_user"
    }
    archive.archive_lineage(lineage_data)
    start = now - timedelta(hours=1)
    end = now + timedelta(hours=1)
    results = archive.query_lineage(start, end)
    assert len(results) == 1
    assert results[0]["table_name"] == "catalog.schema.table1"


def test_query_lineage_with_table_filter(archive):
    now = datetime.now()
    archive.archive_lineage({
        "event_time": now,
        "table_name": "catalog.schema.table1",
        "operation_type": "INSERT",
        "user_name": "user1"
    })
    archive.archive_lineage({
        "event_time": now,
        "table_name": "catalog.schema.table2",
        "operation_type": "UPDATE",
        "user_name": "user2"
    })
    start = now - timedelta(hours=1)
    end = now + timedelta(hours=1)
    results = archive.query_lineage(start, end, "catalog.schema.table1")
    assert len(results) == 1
    assert results[0]["table_name"] == "catalog.schema.table1"


def test_get_table_history(archive):
    table_name = "catalog.schema.table1"
    archive.archive_lineage({
        "event_time": datetime.now(),
        "table_name": table_name,
        "operation_type": "INSERT",
        "user_name": "user1"
    })
    history = archive.get_table_history(table_name)
    assert len(history) >= 1
    assert history[0]["table_name"] == table_name
