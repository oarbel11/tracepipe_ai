import pytest
from unified_lineage import LineageGraph, LineageNode, LineageEdge
from unified_lineage import ColumnNode
from lineage_extractor import LineageExtractor


def test_lineage_graph_creation():
    graph = LineageGraph()
    assert graph is not None
    assert len(graph.nodes) == 0
    assert len(graph.edges) == 0


def test_add_nodes_and_edges():
    graph = LineageGraph()
    node1 = LineageNode(id="n1", node_type="table", name="table1")
    node2 = LineageNode(id="n2", node_type="table", name="table2")
    edge = LineageEdge(source=node1, target=node2)
    
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge(edge)
    
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1


def test_get_upstream():
    graph = LineageGraph()
    n1 = LineageNode(id="n1", node_type="table", name="source")
    n2 = LineageNode(id="n2", node_type="table", name="intermediate")
    n3 = LineageNode(id="n3", node_type="table", name="target")
    
    graph.add_edge(LineageEdge(source=n1, target=n2))
    graph.add_edge(LineageEdge(source=n2, target=n3))
    
    upstream = graph.get_upstream(n3)
    assert len(upstream) == 2
    assert n2 in upstream
    assert n1 in upstream


def test_get_downstream():
    graph = LineageGraph()
    n1 = LineageNode(id="n1", node_type="table", name="source")
    n2 = LineageNode(id="n2", node_type="table", name="intermediate")
    n3 = LineageNode(id="n3", node_type="table", name="target")
    
    graph.add_edge(LineageEdge(source=n1, target=n2))
    graph.add_edge(LineageEdge(source=n2, target=n3))
    
    downstream = graph.get_downstream(n1)
    assert len(downstream) == 2
    assert n2 in downstream
    assert n3 in downstream


def test_column_node_has_dataframe():
    col_node = ColumnNode(
        id="col1", 
        node_type="column", 
        name="user_id",
        column_name="user_id",
        dataframe="users_table"
    )
    assert hasattr(col_node, 'dataframe')
    assert col_node.dataframe == "users_table"


def test_lineage_extractor_build_lineage():
    extractor = LineageExtractor()
    assert hasattr(extractor, 'build_lineage')
    
    plan = {
        'type': 'table',
        'id': 't1',
        'name': 'source_table',
        'children': []
    }
    
    graph = extractor.build_lineage(plan)
    assert isinstance(graph, LineageGraph)
    assert len(graph.nodes) > 0
