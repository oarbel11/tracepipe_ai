"""Tests for Databricks lineage extraction."""
import pytest
from scripts.databricks_lineage.lineage_extractor import (
    DatabricksLineageExtractor
)


class TestDatabricksLineageExtractor:
    """Test suite for DatabricksLineageExtractor."""

    @pytest.fixture
    def mock_http_client(self):
        """Mock HTTP client for testing."""
        def client(endpoint: str):
            if 'jobs/list' in endpoint:
                return {
                    'jobs': [
                        {'job_id': 123, 'settings': {'name': 'ETL Job'}}
                    ]
                }
            elif 'workspace/list' in endpoint:
                return {
                    'objects': [
                        {'object_type': 'NOTEBOOK', 'path': '/Users/test.py'}
                    ]
                }
            return {}
        return client

    @pytest.fixture
    def extractor(self, mock_http_client):
        """Create extractor with mocked HTTP client."""
        return DatabricksLineageExtractor(
            'https://test.databricks.com',
            'test-token',
            http_client=mock_http_client
        )

    def test_extract_lineage(self, extractor):
        """Test basic lineage extraction."""
        lineage = extractor.extract_lineage()
        assert 'nodes' in lineage
        assert 'edges' in lineage
        assert len(lineage['nodes']) == 2

    def test_parse_sql_lineage(self, extractor):
        """Test SQL parsing for table lineage."""
        sql = "INSERT INTO target_table SELECT * FROM source_table"
        result = extractor._parse_sql_lineage(sql)
        assert 'source_table' in result['inputs']
        assert 'target_table' in result['outputs']

    def test_jobs_extraction(self, extractor):
        """Test job node extraction."""
        lineage = extractor.extract_lineage()
        job_nodes = [n for n in lineage['nodes'] if n['type'] == 'job']
        assert len(job_nodes) == 1
        assert job_nodes[0]['name'] == 'ETL Job'

    def test_notebook_extraction(self, extractor):
        """Test notebook node extraction."""
        lineage = extractor.extract_lineage()
        nb_nodes = [n for n in lineage['nodes'] if n['type'] == 'notebook']
        assert len(nb_nodes) == 1
        assert '/Users/test.py' in nb_nodes[0]['name']
