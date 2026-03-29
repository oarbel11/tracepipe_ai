"""Tests for Databricks lineage extraction."""
import pytest
from unittest.mock import Mock, MagicMock, patch
from scripts.databricks_lineage.lineage_extractor import DatabricksLineageExtractor
from scripts.databricks_lineage.lineage_graph import LineageGraphBuilder


@pytest.fixture
def mock_workspace_client():
    """Mock Databricks WorkspaceClient."""
    with patch("scripts.databricks_lineage.lineage_extractor.WorkspaceClient") as mock:
        client = Mock()
        mock.return_value = client
        
        # Mock jobs
        job = Mock()
        job.job_id = 123
        job.settings = Mock()
        job.settings.name = "test_job"
        client.jobs.list.return_value = [job]
        
        job_detail = Mock()
        job_detail.settings = Mock()
        task = Mock()
        task.task_key = "task1"
        task.notebook_task = Mock()
        task.notebook_task.notebook_path = "/test/notebook"
        job_detail.settings.tasks = [task]
        client.jobs.get.return_value = job_detail
        
        # Mock notebooks
        notebook = Mock()
        notebook.object_type = Mock()
        notebook.object_type.name = "NOTEBOOK"
        notebook.path = "/test/notebook"
        client.workspace.list.return_value = [notebook]
        
        # Mock tables
        catalog = Mock()
        catalog.name = "main"
        client.catalogs.list.return_value = [catalog]
        
        schema = Mock()
        schema.name = "default"
        client.schemas.list.return_value = [schema]
        
        table = Mock()
        table.full_name = "main.default.test_table"
        table.name = "test_table"
        client.tables.list.return_value = [table]
        
        yield client


def test_lineage_extractor_jobs(mock_workspace_client):
    """Test job lineage extraction."""
    extractor = DatabricksLineageExtractor()
    jobs = extractor.extract_jobs_lineage()
    
    assert len(jobs) == 1
    assert jobs[0]["type"] == "job"
    assert jobs[0]["id"] == "123"
    assert jobs[0]["name"] == "test_job"
    assert len(jobs[0]["tasks"]) == 1


def test_lineage_extractor_notebooks(mock_workspace_client):
    """Test notebook lineage extraction."""
    extractor = DatabricksLineageExtractor()
    notebooks = extractor.extract_notebooks_lineage()
    
    assert len(notebooks) == 1
    assert notebooks[0]["type"] == "notebook"
    assert notebooks[0]["path"] == "/test/notebook"


def test_lineage_graph_builder():
    """Test lineage graph building."""
    builder = LineageGraphBuilder()
    jobs = [{"id": "1", "name": "job1", "tasks": []}]
    notebooks = [{"id": "nb1", "path": "/nb1"}]
    tables = [{"id": "t1", "name": "table1", "catalog": "c", "schema": "s"}]
    
    graph = builder.build(jobs, notebooks, tables)
    
    assert graph.number_of_nodes() == 3
