import pytest
import networkx as nx
from scripts.lineage_unification import LineageUnifier, LineageIngestion
from config.workspace_config import WorkspaceConfig
from unittest.mock import Mock, patch, MagicMock

def test_lineage_unifier_add_data():
    unifier = LineageUnifier()
    lineage_data = [
        {
            'workspace': 'prod',
            'metastore': 'metastore_prod',
            'catalog': 'catalog1',
            'schema': 'schema1',
            'table': 'table1',
            'upstream_table': 'source_table',
            'upstream_catalog': 'catalog0'
        }
    ]
    unifier.add_lineage_data(lineage_data)
    graph = unifier.get_unified_graph()
    assert graph.number_of_nodes() == 2
    assert graph.number_of_edges() == 1

def test_lineage_unifier_cross_workspace():
    unifier = LineageUnifier()
    lineage_data = [
        {
            'workspace': 'prod',
            'metastore': 'meta1',
            'catalog': 'cat1',
            'schema': 'sch1',
            'table': 'tbl1',
            'upstream_table': 'src',
            'upstream_catalog': 'cat0'
        },
        {
            'workspace': 'dev',
            'metastore': 'meta2',
            'catalog': 'cat2',
            'schema': 'sch2',
            'table': 'tbl2',
            'upstream_table': 'src',
            'upstream_catalog': 'cat0'
        }
    ]
    unifier.add_lineage_data(lineage_data)
    paths = unifier.find_cross_workspace_lineage('cat1')
    assert len(paths) >= 1

def test_workspace_config_add():
    config = WorkspaceConfig(config_path="test_config.yml")
    config.add_workspace(
        name='test_ws',
        host='test.cloud.databricks.com',
        token='test_token',
        metastore_id='meta_123',
        workspace_id='ws_123'
    )
    workspaces = config.get_all_workspaces()
    assert len(workspaces) == 1
    assert workspaces[0]['name'] == 'test_ws'

@patch('scripts.lineage_unification.sql.connect')
def test_lineage_ingestion(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ('cat1', 'sch1', 'tbl1', 'src_tbl', 'cat0')
    ]
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.__enter__.return_value = mock_connection
    
    workspace_config = {
        'name': 'test',
        'host': 'test.databricks.com',
        'token': 'token',
        'metastore_id': 'meta1',
        'workspace_id': 'ws1'
    }
    ingestion = LineageIngestion(workspace_config)
    lineage = ingestion.fetch_lineage()
    assert len(lineage) == 1
    assert lineage[0]['table'] == 'tbl1'
