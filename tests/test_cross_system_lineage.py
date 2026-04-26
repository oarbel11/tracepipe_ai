import pytest
from scripts.lineage_graph import LineageGraph, LineageNode, NodeType
from scripts.cross_system_lineage import CrossSystemLineage


def test_node_type_enum():
    assert NodeType.TABLE.value == "table"
    assert NodeType.VIEW.value == "view"
    assert NodeType.FILE.value == "file"


def test_lineage_node_creation():
    node = LineageNode(
        id="test_table",
        name="test_table",
        node_type=NodeType.TABLE,
        metadata={"catalog": "main"}
    )
    assert node.id == "test_table"
    assert node.node_type == NodeType.TABLE


def test_lineage_graph_add_node():
    graph = LineageGraph()
    node = LineageNode("t1", "table1", NodeType.TABLE)
    graph.add_node(node)
    assert graph.get_node("t1") == node


def test_lineage_graph_add_edge():
    graph = LineageGraph()
    graph.add_node(LineageNode("t1", "table1", NodeType.TABLE))
    graph.add_node(LineageNode("t2", "table2", NodeType.TABLE))
    graph.add_edge("t1", "t2")
    assert "t2" in graph.edges["t1"]


def test_get_downstream():
    graph = LineageGraph()
    graph.add_node(LineageNode("t1", "table1", NodeType.TABLE))
    graph.add_node(LineageNode("t2", "table2", NodeType.TABLE))
    graph.add_node(LineageNode("t3", "table3", NodeType.TABLE))
    graph.add_edge("t1", "t2")
    graph.add_edge("t2", "t3")
    downstream = graph.get_downstream("t1")
    assert "t2" in downstream
    assert "t3" in downstream


def test_get_upstream():
    graph = LineageGraph()
    graph.add_node(LineageNode("t1", "table1", NodeType.TABLE))
    graph.add_node(LineageNode("t2", "table2", NodeType.TABLE))
    graph.add_edge("t1", "t2")
    upstream = graph.get_upstream("t2")
    assert "t1" in upstream


def test_cross_system_add_table():
    lineage = CrossSystemLineage()
    lineage.add_table("catalog.schema.table1")
    node = lineage.graph.get_node("catalog.schema.table1")
    assert node is not None
    assert node.node_type == NodeType.TABLE


def test_cross_system_impact_analysis():
    lineage = CrossSystemLineage()
    lineage.add_table("t1")
    lineage.add_table("t2")
    lineage.add_dependency("t1", "t2")
    result = lineage.get_impact_analysis("t1")
    assert result["downstream_count"] == 1
    assert "t2" in result["downstream"]


def test_handle_table_rename():
    lineage = CrossSystemLineage()
    lineage.add_table("old_table")
    lineage.add_table("downstream_table")
    lineage.add_dependency("old_table", "downstream_table")
    lineage.handle_table_rename("old_table", "new_table")
    result = lineage.get_impact_analysis("new_table")
    assert "downstream_table" in result["downstream"]
