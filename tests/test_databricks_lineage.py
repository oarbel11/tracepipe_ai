import pytest
from unittest.mock import Mock, patch, MagicMock
from scripts.databricks_lineage.lineage_extractor import DatabricksLineageExtractor
from scripts.databricks_lineage.lineage_graph import LineageGraphBuilder
import json
import tempfile
import os


class TestDatabricksLineageExtractor:
    def test_init(self):
        extractor = DatabricksLineageExtractor(
            host='https://example.databricks.com',
            token='test_token',
            http_path='/sql/1.0/warehouses/abc'
        )
        assert extractor.host == 'https://example.databricks.com'
        assert extractor.token == 'test_token'
        assert extractor.http_path == '/sql/1.0/warehouses/abc'

    @patch('scripts.databricks_lineage.lineage_extractor.requests.get')
    def test_extract_jobs(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'jobs': [
                {'job_id': 123, 'settings': {'name': 'ETL Job', 'tasks': []}}
            ]
        }
        mock_get.return_value = mock_response
        
        extractor = DatabricksLineageExtractor('https://example.databricks.com', 'token')
        jobs = extractor.extract_jobs()
        
        assert len(jobs) == 1
        assert jobs[0]['job_id'] == 123
        assert jobs[0]['name'] == 'ETL Job'

    @patch('scripts.databricks_lineage.lineage_extractor.requests.get')
    def test_extract_dlt_pipelines(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            'statuses': [
                {'pipeline_id': 'pipe123', 'name': 'DLT Pipeline', 'state': 'RUNNING'}
            ]
        }
        mock_get.return_value = mock_response
        
        extractor = DatabricksLineageExtractor('https://example.databricks.com', 'token')
        pipelines = extractor.extract_dlt_pipelines()
        
        assert len(pipelines) == 1
        assert pipelines[0]['pipeline_id'] == 'pipe123'
        assert pipelines[0]['state'] == 'RUNNING'


class TestLineageGraphBuilder:
    def test_add_table_lineage(self):
        builder = LineageGraphBuilder()
        lineage = [
            {'source_catalog': 'cat1', 'source_schema': 'sch1', 'source_table': 'tbl1',
             'target_catalog': 'cat2', 'target_schema': 'sch2', 'target_table': 'tbl2'}
        ]
        builder.add_table_lineage(lineage)
        
        assert builder.graph.number_of_nodes() == 2
        assert builder.graph.number_of_edges() == 1

    def test_add_job_lineage(self):
        builder = LineageGraphBuilder()
        jobs = [{'job_id': 123, 'name': 'Test Job', 'tasks': [
            {'notebook_task': {'notebook_path': '/Users/test/notebook'}}
        ]}]
        builder.add_job_lineage(jobs)
        
        assert builder.graph.number_of_nodes() == 2
        assert any('job:123' in node for node in builder.graph.nodes())

    def test_export_json(self):
        builder = LineageGraphBuilder()
        builder.add_table_lineage([{
            'source_catalog': 'c1', 'source_schema': 's1', 'source_table': 't1',
            'target_catalog': 'c2', 'target_schema': 's2', 'target_table': 't2'
        }])
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_path = f.name
        
        try:
            builder.export_json(temp_path)
            assert os.path.exists(temp_path)
            
            with open(temp_path, 'r') as f:
                data = json.load(f)
                assert 'nodes' in data
                assert 'links' in data
        finally:
            os.unlink(temp_path)

    def test_get_stats(self):
        builder = LineageGraphBuilder()
        builder.add_table_lineage([{
            'source_catalog': 'c1', 'source_schema': 's1', 'source_table': 't1',
            'target_catalog': 'c2', 'target_schema': 's2', 'target_table': 't2'
        }])
        builder.add_job_lineage([{'job_id': 1, 'name': 'Job1', 'tasks': []}])
        
        stats = builder.get_stats()
        assert stats['tables'] == 2
        assert stats['jobs'] == 1
        assert stats['total_nodes'] == 3
