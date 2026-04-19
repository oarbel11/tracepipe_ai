import pytest
from scripts.unified_lineage import UnifiedLineageGraph, LineageNode
from scripts.lineage_aggregator import LineageAggregator, ExternalLineageConnector

def test_lineage_node_creation():
    node = LineageNode(
        node_id="test.table.users",
        node_type="table",
        platform="databricks",
        tags=["pii"]
    )
    assert node.node_id == "test.table.users"
    assert node.platform == "databricks"
    assert "pii" in node.tags

def test_unified_graph_add_node():
    graph = UnifiedLineageGraph()
    node = LineageNode("table1", "table", "databricks")
    graph.add_node(node)
    assert "table1" in graph.nodes
    assert graph.graph.has_node("table1")

def test_unified_graph_lineage():
    graph = UnifiedLineageGraph()
    node1 = LineageNode("source", "table", "databricks")
    node2 = LineageNode("target", "table", "databricks")
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge("source", "target")
    upstream = graph.get_upstream("target")
    assert "source" in upstream
    downstream = graph.get_downstream_impact("source")
    assert "target" in downstream

def test_lineage_aggregator_databricks():
    agg = LineageAggregator()
    agg.add_databricks_lineage("catalog.schema.table", upstream=["catalog.schema.source"])
    graph = agg.get_unified_graph()
    assert "catalog.schema.table" in graph.nodes
    assert "catalog.schema.source" in graph.nodes

def test_lineage_aggregator_external():
    agg = LineageAggregator()
    agg.add_databricks_lineage("db.main.users")
    agg.add_external_lineage("tableau", "dashboard_1", upstream=["db.main.users"])
    graph = agg.get_unified_graph()
    assert "tableau://dashboard_1" in graph.nodes
    downstream = graph.get_downstream_impact("db.main.users")
    assert "tableau://dashboard_1" in downstream

def test_cross_platform_impact():
    agg = LineageAggregator()
    agg.add_databricks_lineage("companies_data.main.entities")
    agg.add_external_lineage("powerbi", "report_1", upstream=["companies_data.main.entities"])
    agg.add_external_lineage("airflow", "dag_1", downstream=["companies_data.main.entities"])
    impact = agg.get_cross_platform_impact("companies_data.main.entities")
    assert "powerbi" in impact

def test_graph_export():
    agg = LineageAggregator()
    agg.add_databricks_lineage("test.table1")
    graph_dict = agg.get_unified_graph().to_dict()
    assert "nodes" in graph_dict
    assert "edges" in graph_dict
    assert len(graph_dict["nodes"]) > 0
