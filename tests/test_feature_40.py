import pytest
from unified_lineage import LineageGraph, LineageNode, LineageEdge
from unified_lineage import LineageExtractor, UnityCatalogExtractor

def test_lineage_graph_creation():
    graph = LineageGraph()
    node1 = LineageNode(id='table1', type='table')
    node2 = LineageNode(id='table2', type='table')
    graph.add_node(node1)
    graph.add_node(node2)
    edge = LineageEdge(source='table1', target='table2', type='data_flow')
    graph.add_edge(edge)
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1

def test_unity_catalog_extractor():
    extractor = UnityCatalogExtractor('https://workspace.cloud.databricks.com', 'token')
    graph = extractor.extract_table_lineage('catalog.schema.table')
    assert 'catalog.schema.table' in graph.nodes

def test_spark_lineage_extractor():
    extractor = LineageExtractor()
    graph = extractor.build_lineage('df1', ['col1', 'col2'])
    assert 'df1.col1' in graph.nodes
    assert 'df1.col2' in graph.nodes

def test_cross_platform_merge():
    uc_extractor = UnityCatalogExtractor('url', 'token')
    uc_graph = uc_extractor.extract_table_lineage('uc_table')
    spark_extractor = LineageExtractor()
    spark_graph = spark_extractor.build_lineage('df1', ['col1'])
    uc_graph.merge(spark_graph)
    assert 'uc_table' in uc_graph.nodes
    assert 'df1.col1' in uc_graph.nodes

def test_end_to_end_lineage():
    graph = LineageGraph()
    source = LineageNode(id='source_table', type='table')
    transform = LineageNode(id='transform_df', type='dataframe')
    target = LineageNode(id='target_table', type='table')
    graph.add_node(source)
    graph.add_node(transform)
    graph.add_node(target)
    graph.add_edge(LineageEdge(source='source_table', target='transform_df', type='read'))
    graph.add_edge(LineageEdge(source='transform_df', target='target_table', type='write'))
    upstream = graph.get_upstream('target_table')
    assert 'source_table' in upstream
    assert 'transform_df' in upstream
