import pytest
import os
import tempfile
from datetime import datetime, timedelta
from tracepipe_ai.lineage_archive import LineageArchive
from tracepipe_ai.databricks_collector import DatabricksLineageCollector


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)


def test_lineage_archive_init(temp_db):
    archive = LineageArchive(db_path=temp_db)
    assert archive is not None
    stats = archive.get_statistics()
    assert stats["total"] == 0
    archive.close()


def test_archive_lineage_events(temp_db):
    archive = LineageArchive(db_path=temp_db)
    events = [
        {
            "id": "event1",
            "event_type": "table_read",
            "source_table": "catalog.schema.table1",
            "target_table": "catalog.schema.table2",
            "timestamp": datetime.now(),
            "metadata": {"user": "test_user"}
        }
    ]
    count = archive.archive_lineage(events)
    assert count == 1
    stats = archive.get_statistics()
    assert stats["total"] == 1
    archive.close()


def test_query_historical_lineage(temp_db):
    archive = LineageArchive(db_path=temp_db)
    now = datetime.now()
    events = [
        {
            "id": "event1",
            "event_type": "table_read",
            "source_table": "catalog.schema.table1",
            "target_table": "catalog.schema.table2",
            "timestamp": now - timedelta(days=2),
            "metadata": {}
        },
        {
            "id": "event2",
            "event_type": "table_write",
            "source_table": "catalog.schema.table2",
            "target_table": "catalog.schema.table3",
            "timestamp": now - timedelta(days=1),
            "metadata": {}
        }
    ]
    archive.archive_lineage(events)
    results = archive.query_historical_lineage(
        start_date=now - timedelta(days=3),
        end_date=now
    )
    assert len(results) == 2
    archive.close()


def test_databricks_collector_init():
    collector = DatabricksLineageCollector()
    assert collector is not None
    assert not collector.is_configured()
