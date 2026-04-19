import pytest
from scripts.unified_lineage import UnifiedLineageGraph, LineageNode
from scripts.lineage_aggregator import LineageAggregator, ExternalLineageConnector, BIToolConnector


def test_lineage_node_creation():
    node = LineageNode("table1", "table", "databricks", {"catalog": "main"})
    assert node.node_id == "table1"
    assert node.node_type == "table"
    assert node.platform == "databricks"
    assert node.metadata["catalog"] == "main"


def test_unified_graph_add_nodes():
    graph = UnifiedLineageGraph()
    node1 = LineageNode("table1", "table", "databricks")
    node2 = LineageNode("table2", "table", "databricks")
    graph.add_node(node1)
    graph.add_node(node2)
    assert "table1" in graph.graph.nodes
    assert "table2" in graph.graph.nodes


def test_unified_graph_add_edges():
    graph = UnifiedLineageGraph()
    node1 = LineageNode("table1", "table", "databricks")
    node2 = LineageNode("table2", "table", "databricks")
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge("table1", "table2", "transforms")
    assert graph.graph.has_edge("table1", "table2")


def test_get_upstream_downstream():
    graph = UnifiedLineageGraph()
    for i in range(3):
        node = LineageNode(f"table{i}", "table", "databricks")
        graph.add_node(node)
    graph.add_edge("table0", "table1")
    graph.add_edge("table1", "table2")
    assert graph.get_upstream("table1") == ["table0"]
    assert graph.get_downstream("table1") == ["table2"]


def test_impact_analysis():
    graph = UnifiedLineageGraph()
    for i in range(3):
        node = LineageNode(f"table{i}", "table", "databricks")
        graph.add_node(node)
    graph.add_edge("table0", "table1")
    graph.add_edge("table1", "table2")
    impact = graph.get_impact_analysis("table1")
    assert "table0" in impact["upstream"]
    assert "table2" in impact["downstream"]


def test_lineage_aggregator():
    aggregator = LineageAggregator()
    uc_lineage = {"nodes": [{"id": "uc_table1", "type": "table"}], "edges": []}
    aggregator.add_unity_catalog_lineage(uc_lineage)
    graph = aggregator.get_unified_graph()
    assert "uc_table1" in graph.graph.nodes


def test_external_connector():
    connector = BIToolConnector("tableau", "https://api.tableau.com")
    lineage = connector.fetch_lineage()
    assert len(lineage["nodes"]) > 0
    assert lineage["nodes"][0]["platform"] == "tableau"
