import pytest
from scripts.unified_lineage import UnifiedLineageGraph, LineageNode, LineageEdge, NodeType

def test_unified_lineage_graph_creation():
    graph = UnifiedLineageGraph()
    assert graph is not None
    assert len(graph.nodes) == 0
    assert len(graph.edges) == 0

def test_add_nodes():
    graph = UnifiedLineageGraph()
    node1 = LineageNode(id="node1", name="Table1", node_type=NodeType.TABLE, workspace="ws1")
    node2 = LineageNode(id="node2", name="Table2", node_type=NodeType.TABLE, workspace="ws2")
    graph.add_node(node1)
    graph.add_node(node2)
    assert len(graph.nodes) == 2
    assert "node1" in graph.nodes
    assert "node2" in graph.nodes

def test_add_edges():
    graph = UnifiedLineageGraph()
    node1 = LineageNode(id="node1", name="Table1", node_type=NodeType.TABLE)
    node2 = LineageNode(id="node2", name="Table2", node_type=NodeType.TABLE)
    graph.add_node(node1)
    graph.add_node(node2)
    edge = LineageEdge(source_id="node1", target_id="node2")
    graph.add_edge(edge)
    assert len(graph.edges) == 1

def test_get_upstream():
    graph = UnifiedLineageGraph()
    node1 = LineageNode(id="node1", name="Table1", node_type=NodeType.TABLE)
    node2 = LineageNode(id="node2", name="Table2", node_type=NodeType.TABLE)
    node3 = LineageNode(id="node3", name="Table3", node_type=NodeType.TABLE)
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_node(node3)
    graph.add_edge(LineageEdge(source_id="node1", target_id="node2"))
    graph.add_edge(LineageEdge(source_id="node2", target_id="node3"))
    upstream = graph.get_upstream("node3")
    assert "node2" in upstream
    assert "node1" in upstream

def test_get_downstream():
    graph = UnifiedLineageGraph()
    node1 = LineageNode(id="node1", name="Table1", node_type=NodeType.TABLE)
    node2 = LineageNode(id="node2", name="Table2", node_type=NodeType.TABLE)
    node3 = LineageNode(id="node3", name="Table3", node_type=NodeType.TABLE)
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_node(node3)
    graph.add_edge(LineageEdge(source_id="node1", target_id="node2"))
    graph.add_edge(LineageEdge(source_id="node2", target_id="node3"))
    downstream = graph.get_downstream("node1")
    assert "node2" in downstream
    assert "node3" in downstream

def test_cross_workspace_lineage():
    graph = UnifiedLineageGraph()
    node1 = LineageNode(id="ws1.table1", name="Table1", node_type=NodeType.TABLE, workspace="ws1")
    node2 = LineageNode(id="ws2.table2", name="Table2", node_type=NodeType.TABLE, workspace="ws2")
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge(LineageEdge(source_id="ws1.table1", target_id="ws2.table2"))
    downstream = graph.get_downstream("ws1.table1")
    assert "ws2.table2" in downstream
