import pytest
from unified_lineage import LineageGraph, LineageNode, LineageEdge
from lineage_extractor import LineageExtractor, ColumnNode


def test_lineage_graph():
    graph = LineageGraph()
    node1 = LineageNode("n1", "table")
    node2 = LineageNode("n2", "table")
    graph.add_node(node1)
    graph.add_node(node2)
    edge = LineageEdge("n1", "n2")
    graph.add_edge(edge)
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1


def test_upstream_downstream():
    graph = LineageGraph()
    graph.add_node(LineageNode("n1", "table"))
    graph.add_node(LineageNode("n2", "table"))
    graph.add_node(LineageNode("n3", "table"))
    graph.add_edge(LineageEdge("n1", "n2"))
    graph.add_edge(LineageEdge("n2", "n3"))
    upstream = graph.get_upstream("n3")
    assert "n2" in upstream
    assert "n1" in upstream
    downstream = graph.get_downstream("n1")
    assert "n2" in downstream
    assert "n3" in downstream


def test_column_node():
    node = ColumnNode("df1.col1", "col1", "df1")
    assert node.column_name == "col1"
    assert node.dataframe == "df1"


def test_lineage_extractor():
    extractor = LineageExtractor()
    ops = [{"type": "select", "source": "df1", "target": "df2", "columns": ["col1"]}]
    graph = extractor.build_lineage(ops)
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
