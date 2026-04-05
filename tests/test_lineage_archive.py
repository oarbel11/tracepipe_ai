import pytest
from datetime import datetime, timedelta
import os
import tempfile
from tracepipe_ai.lineage_archive import LineageArchive


@pytest.fixture
def archive():
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp:
        db_path = tmp.name
    archive = LineageArchive(db_path)
    yield archive
    archive.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


def test_archive_lineage(archive):
    lineage_data = {
        "event_id": "test_001",
        "event_type": "table_read",
        "source_table": "sales.customers",
        "target_table": "analytics.customer_summary",
        "transformation": "SELECT * FROM sales.customers",
        "timestamp": datetime.utcnow(),
        "metadata": {"user": "analyst@company.com"}
    }
    event_id = archive.archive_lineage(lineage_data)
    assert event_id == "test_001"


def test_query_historical_lineage(archive):
    now = datetime.utcnow()
    lineage_data = {
        "event_id": "test_002",
        "event_type": "table_write",
        "source_table": "raw.transactions",
        "target_table": "processed.transactions",
        "timestamp": now,
        "metadata": {}
    }
    archive.archive_lineage(lineage_data)
    results = archive.query_historical_lineage(
        now - timedelta(hours=1),
        now + timedelta(hours=1)
    )
    assert len(results) == 1
    assert results[0]["event_id"] == "test_002"


def test_query_by_table_name(archive):
    now = datetime.utcnow()
    archive.archive_lineage({
        "event_id": "test_003",
        "event_type": "table_read",
        "source_table": "db.table_a",
        "target_table": "db.table_b",
        "timestamp": now
    })
    results = archive.query_historical_lineage(
        now - timedelta(hours=1),
        now + timedelta(hours=1),
        table_name="db.table_a"
    )
    assert len(results) == 1


def test_compliance_report(archive):
    now = datetime.utcnow()
    for i in range(5):
        archive.archive_lineage({
            "event_id": f"test_{i}",
            "event_type": "transform",
            "source_table": f"src_{i}",
            "target_table": f"tgt_{i}",
            "timestamp": now
        })
    report = archive.get_compliance_report(
        now - timedelta(hours=1),
        now + timedelta(hours=1)
    )
    assert report["total_events"] == 5
    assert report["unique_sources"] == 5
    assert report["unique_targets"] == 5
