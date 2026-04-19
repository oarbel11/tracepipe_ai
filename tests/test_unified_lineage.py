import pytest
from scripts.unified_lineage import UnifiedLineageGraph, LineageNode, LineageEdge, NodeType

def test_unified_lineage_add_node():
    graph = UnifiedLineageGraph()
    node = LineageNode("table1", "my_table", NodeType.UC_TABLE)
    graph.add_node(node)
    assert "table1" in graph.nodes
    assert graph.nodes["table1"].name == "my_table"

def test_unified_lineage_add_edge():
    graph = UnifiedLineageGraph()
    node1 = LineageNode("table1", "source", NodeType.UC_TABLE)
    node2 = LineageNode("table2", "target", NodeType.UC_TABLE)
    graph.add_node(node1)
    graph.add_node(node2)
    edge = LineageEdge("table1", "table2")
    graph.add_edge(edge)
    assert graph.graph.has_edge("table1", "table2")

def test_get_upstream():
    graph = UnifiedLineageGraph()
    for i in range(3):
        graph.add_node(LineageNode(f"t{i}", f"table{i}", NodeType.UC_TABLE))
    graph.add_edge(LineageEdge("t0", "t1"))
    graph.add_edge(LineageEdge("t1", "t2"))
    upstream = graph.get_upstream("t2")
    assert "t0" in upstream
    assert "t1" in upstream

def test_get_downstream():
    graph = UnifiedLineageGraph()
    for i in range(3):
        graph.add_node(LineageNode(f"t{i}", f"table{i}", NodeType.UC_TABLE))
    graph.add_edge(LineageEdge("t0", "t1"))
    graph.add_edge(LineageEdge("t1", "t2"))
    downstream = graph.get_downstream("t0")
    assert "t1" in downstream
    assert "t2" in downstream

def test_impact_analysis():
    graph = UnifiedLineageGraph()
    for i in range(3):
        graph.add_node(LineageNode(f"t{i}", f"table{i}", NodeType.UC_TABLE))
    graph.add_edge(LineageEdge("t0", "t1"))
    graph.add_edge(LineageEdge("t1", "t2"))
    impact = graph.get_impact_analysis("t1")
    assert impact["upstream_count"] == 1
    assert impact["downstream_count"] == 1

def test_merge_from_unity_catalog():
    graph = UnifiedLineageGraph()
    uc_data = {
        "tables": [{"id": "t1", "name": "table1", "metadata": {}}],
        "edges": [{"source": "t0", "target": "t1"}]
    }
    graph.merge_from_unity_catalog(uc_data)
    assert "t1" in graph.nodes
    assert graph.graph.has_edge("t0", "t1")
