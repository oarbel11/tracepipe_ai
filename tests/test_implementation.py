import pytest
from tracepipe_ai.lineage import UnifiedLineageGraph, LineageExtractor, ColumnNode


def test_unified_lineage_graph():
    graph = UnifiedLineageGraph()
    graph.add_node('node1', {'type': 'table'})
    graph.add_node('node2', {'type': 'table'})
    graph.add_edge('node1', 'node2')
    
    assert 'node1' in graph.nodes
    assert 'node2' in graph.nodes
    assert len(graph.edges) == 1


def test_lineage_extractor():
    extractor = LineageExtractor()
    plan = "Project [col1, col2]\nRelation [col1, col2]"
    result = extractor.extract_from_plan(plan)
    
    assert 'col1' in result
    assert 'col2' in result


def test_column_node():
    node = ColumnNode('col1', dataframe='df1')
    assert node.column == 'col1'
    assert node.dataframe == 'df1'


def test_upstream_downstream():
    graph = UnifiedLineageGraph()
    graph.add_node('node1')
    graph.add_node('node2')
    graph.add_node('node3')
    graph.add_edge('node1', 'node2')
    graph.add_edge('node2', 'node3')
    
    upstream = graph.get_upstream('node2')
    assert 'node1' in upstream
    
    downstream = graph.get_downstream('node2')
    assert 'node3' in downstream
