"""Tests for long-term lineage archiving."""
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from tracepipe_ai.lineage_archive import LineageArchive


def test_archive_initialization():
    """Test archive database initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/test.duckdb"
        archive = LineageArchive(db_path)
        assert Path(db_path).exists()
        archive.close()


def test_archive_lineage_events():
    """Test archiving lineage events."""
    with tempfile.TemporaryDirectory() as tmpdir:
        archive = LineageArchive(f"{tmpdir}/test.duckdb")
        
        events = [
            {
                'event_time': datetime.now(),
                'source_table': 'catalog.schema.table1',
                'target_table': 'catalog.schema.table2',
                'operation': 'INSERT',
                'metadata': {'user': 'test_user'}
            },
            {
                'event_time': datetime.now() - timedelta(days=30),
                'source_table': 'catalog.schema.table3',
                'target_table': 'catalog.schema.table4',
                'operation': 'UPDATE',
                'metadata': {'query_id': 'q123'}
            }
        ]
        
        count = archive.archive_lineage(events)
        assert count == 2
        archive.close()


def test_query_historical_lineage():
    """Test querying historical lineage data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        archive = LineageArchive(f"{tmpdir}/test.duckdb")
        
        now = datetime.now()
        events = [
            {
                'event_time': now - timedelta(days=10),
                'source_table': 'cat.sch.src',
                'target_table': 'cat.sch.tgt',
                'operation': 'INSERT',
                'metadata': {}
            }
        ]
        
        archive.archive_lineage(events)
        
        results = archive.query_historical(
            (now - timedelta(days=30)).isoformat(),
            now.isoformat()
        )
        
        assert len(results) == 1
        assert results[0]['source_table'] == 'cat.sch.src'
        archive.close()


def test_query_with_table_filter():
    """Test querying with table name filter."""
    with tempfile.TemporaryDirectory() as tmpdir:
        archive = LineageArchive(f"{tmpdir}/test.duckdb")
        
        now = datetime.now()
        events = [
            {'event_time': now, 'source_table': 'cat.sch.customers',
             'target_table': 'cat.sch.report', 'operation': 'INSERT',
             'metadata': {}},
            {'event_time': now, 'source_table': 'cat.sch.orders',
             'target_table': 'cat.sch.summary', 'operation': 'INSERT',
             'metadata': {}}
        ]
        
        archive.archive_lineage(events)
        results = archive.query_historical(
            (now - timedelta(days=1)).isoformat(),
            (now + timedelta(days=1)).isoformat(),
            'customers'
        )
        
        assert len(results) == 1
        assert 'customers' in results[0]['source_table']
        archive.close()
