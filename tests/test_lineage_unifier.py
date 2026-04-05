import pytest
import networkx as nx
import json
import tempfile
import os
from scripts.lineage_unifier import LineageUnifier
from connectors.dbt_connector import DbtConnector
from connectors import ConnectorRegistry, LineageNode

@pytest.fixture
def mock_dbt_manifest():
    manifest = {
        "nodes": {
            "model.my_project.customers": {
                "resource_type": "model",
                "name": "customers",
                "schema": "analytics",
                "database": "prod",
                "depends_on": {"nodes": ["source.my_project.raw.users"]}
            }
        },
        "sources": {
            "source.my_project.raw.users": {
                "resource_type": "source",
                "name": "users",
                "schema": "raw",
                "database": "prod",
                "depends_on": {"nodes": []}
            }
        }
    }
    
    temp_dir = tempfile.mkdtemp()
    manifest_path = os.path.join(temp_dir, 'manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f)
    
    yield manifest_path
    os.remove(manifest_path)
    os.rmdir(temp_dir)

def test_dbt_connector_extraction(mock_dbt_manifest):
    config = {'manifest_path': mock_dbt_manifest, 'project_name': 'my_project'}
    connector = DbtConnector(config)
    
    assert connector.validate_config()
    graph = connector.extract_lineage()
    
    assert len(graph.nodes()) == 2
    assert len(graph.edges()) == 1

def test_lineage_unifier_integration(mock_dbt_manifest):
    unifier = LineageUnifier()
    config = {'manifest_path': mock_dbt_manifest, 'project_name': 'my_project'}
    
    unifier.add_lineage_source('dbt', config)
    
    assert len(unifier.unified_graph.nodes()) == 2
    assert len(unifier.unified_graph.edges()) == 1

def test_cross_system_mapping():
    unifier = LineageUnifier()
    unifier.unified_graph.add_node('dbt://proj/model1', node_type='model', system='dbt', metadata={'name': 'customers'})
    unifier.unified_graph.add_node('databricks://catalog/table1', node_type='table', system='databricks', metadata={'name': 'customers'})
    
    unifier.add_mapping_rule('dbt://', 'databricks://', 'dbt', 'databricks')
    unifier.apply_cross_system_mappings()
    
    assert len(unifier.unified_graph.edges()) == 1

def test_end_to_end_lineage(mock_dbt_manifest):
    unifier = LineageUnifier()
    config = {'manifest_path': mock_dbt_manifest, 'project_name': 'my_project'}
    unifier.add_lineage_source('dbt', config)
    
    node_id = 'dbt://my_project/model.my_project.customers'
    lineage = unifier.get_end_to_end_lineage(node_id, 'upstream')
    
    assert node_id in lineage.nodes()
    assert len(lineage.nodes()) >= 1
