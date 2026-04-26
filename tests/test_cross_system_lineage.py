import pytest
from tracepipe.cross_system_lineage import LineageNode, LineageGraph, CrossSystemLineage

def test_lineage_node_creation():
    node = LineageNode(node_id='table1', node_type='table', name='customers', metadata={'schema': 'public'})
    assert node.node_id == 'table1'
    assert node.node_type == 'table'
    assert node.name == 'customers'
    assert node.metadata['schema'] == 'public'

def test_lineage_graph_add_node():
    graph = LineageGraph()
    graph.add_node('table1', 'table', 'customers', {'schema': 'public'})
    assert 'table1' in graph.nodes
    assert graph.nodes['table1'].name == 'customers'

def test_lineage_graph_add_edge():
    graph = LineageGraph()
    graph.add_node('table1', 'table', 'source', {})
    graph.add_node('table2', 'table', 'target', {})
    graph.add_edge('table1', 'table2', 'transform', {'job': 'etl1'})
    assert len(graph.edges) == 1
    assert 'table2' in graph.adjacency['table1']

def test_get_downstream():
    graph = LineageGraph()
    graph.add_node('t1', 'table', 'source', {})
    graph.add_node('t2', 'table', 'intermediate', {})
    graph.add_node('t3', 'table', 'target', {})
    graph.add_edge('t1', 't2', 'transform', {})
    graph.add_edge('t2', 't3', 'transform', {})
    downstream = graph.get_downstream('t1')
    assert 't2' in downstream
    assert 't3' in downstream

def test_get_upstream():
    graph = LineageGraph()
    graph.add_node('t1', 'table', 'source', {})
    graph.add_node('t2', 'table', 'intermediate', {})
    graph.add_node('t3', 'table', 'target', {})
    graph.add_edge('t1', 't2', 'transform', {})
    graph.add_edge('t2', 't3', 'transform', {})
    upstream = graph.get_upstream('t3')
    assert 't2' in upstream
    assert 't1' in upstream

def test_cross_system_add_table():
    lineage = CrossSystemLineage()
    lineage.add_table('db.table1', 'customers', {'source': 'mysql'})
    assert 'db.table1' in lineage.graph.nodes

def test_cross_system_impact_analysis():
    lineage = CrossSystemLineage()
    lineage.add_table('source', 'source_table', {})
    lineage.add_table('target', 'target_table', {})
    lineage.graph.add_edge('source', 'target', 'transform', {})
    downstream = lineage.graph.get_downstream('source')
    assert 'target' in downstream

def test_handle_table_rename():
    lineage = CrossSystemLineage()
    lineage.add_table('table1', 'old_name', {})
    lineage.table_renames['old_name'] = 'new_name'
    assert 'old_name' in lineage.table_renames
