import pytest
from scripts.external_connectors import DbtConnector, TableauConnector, SalesforceConnector, LineageNode, LineageEdge
from scripts.lineage_stitcher import LineageStitcher
from scripts.unified_lineage import UnifiedLineageBuilder, UnifiedLineageGraph
import tempfile
import json
import os

def test_lineage_node_creation():
    node = LineageNode(system="dbt", identifier="model.sales", node_type="model")
    assert node.system == "dbt"
    assert node.identifier == "model.sales"

def test_dbt_connector_with_manifest():
    manifest = {
        "nodes": {
            "model.project.sales": {
                "unique_id": "model.project.sales",
                "resource_type": "model",
                "database": "prod",
                "schema": "analytics",
                "name": "sales",
                "depends_on": {"nodes": ["source.project.raw_sales"]}
            }
        }
    }
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        json.dump(manifest, f)
        manifest_path = f.name
    
    try:
        connector = DbtConnector(manifest_path=manifest_path)
        nodes, edges = connector.extract_lineage()
        assert len(nodes) >= 1
        assert any(n.system == "dbt" for n in nodes)
    finally:
        os.unlink(manifest_path)

def test_tableau_connector():
    connector = TableauConnector(server="tableau.test.com", token="test_token")
    nodes, edges = connector.extract_lineage()
    assert len(nodes) >= 1
    assert any(n.system == "tableau" for n in nodes)

def test_lineage_stitcher_matching():
    stitcher = LineageStitcher()
    node1 = LineageNode(system="dbt", identifier="model.sales", node_type="model",
                       metadata={'database': 'prod', 'schema': 'analytics', 'name': 'sales'})
    node2 = LineageNode(system="unity_catalog", identifier="prod.analytics.sales", node_type="table")
    
    assert stitcher.find_matches(node1, node2)

def test_unified_lineage_builder():
    builder = UnifiedLineageBuilder()
    builder.add_connector(TableauConnector(server="test", token="test"))
    builder.add_connector(SalesforceConnector(instance_url="test", access_token="test"))
    
    graph = builder.build_unified_graph()
    assert graph is not None
    assert len(graph.nodes) > 0

def test_downstream_impact():
    builder = UnifiedLineageBuilder()
    builder.add_connector(TableauConnector(server="test", token="test"))
    graph = builder.build_unified_graph()
    
    impact = graph.get_downstream_impact("datasource")
    assert isinstance(impact, list)

def test_trace_path():
    builder = UnifiedLineageBuilder()
    builder.add_connector(TableauConnector(server="test", token="test"))
    graph = builder.build_unified_graph()
    
    path = graph.trace_path("datasource_1", "workbook_1")
    assert isinstance(path, list)
