import unittest
import os
from datetime import datetime, timedelta
import tempfile
from pathlib import Path
import json

from tracepipe_ai.lineage_archive import LineageArchive


class TestLineageArchive(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_archive.duckdb")
        self.archive = LineageArchive(self.db_path)

    def tearDown(self):
        self.archive.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)

    def test_archive_lineage(self):
        lineage_data = {
            "id": "test_table_001",
            "entity_type": "table",
            "entity_name": "catalog.schema.test_table",
            "upstream_entities": ["catalog.schema.source_table"],
            "downstream_entities": ["catalog.schema.target_table"],
            "metadata": {"owner": "test_user", "columns": ["col1", "col2"]},
            "captured_at": datetime.now(),
            "source": "databricks"
        }
        result = self.archive.archive_lineage(lineage_data)
        self.assertTrue(result)

    def test_query_historical_lineage(self):
        now = datetime.now()
        lineage_data = {
            "id": "test_query_001",
            "entity_type": "table",
            "entity_name": "catalog.schema.query_table",
            "upstream_entities": [],
            "downstream_entities": [],
            "metadata": {},
            "captured_at": now,
            "source": "databricks"
        }
        self.archive.archive_lineage(lineage_data)
        
        results = self.archive.query_historical_lineage("catalog.schema.query_table")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["entity_name"], "catalog.schema.query_table")

    def test_query_with_date_range(self):
        now = datetime.now()
        past = now - timedelta(days=30)
        lineage_data = {
            "id": "test_range_001",
            "entity_type": "table",
            "entity_name": "catalog.schema.range_table",
            "upstream_entities": [],
            "downstream_entities": [],
            "metadata": {},
            "captured_at": past,
            "source": "databricks"
        }
        self.archive.archive_lineage(lineage_data)
        
        results = self.archive.query_historical_lineage(
            "catalog.schema.range_table",
            start_date=past - timedelta(days=1),
            end_date=now
        )
        self.assertEqual(len(results), 1)
