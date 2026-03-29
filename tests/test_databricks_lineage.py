import pytest
import networkx as nx
from scripts.databricks_lineage_extractor import DatabricksLineageExtractor
from scripts.lineage_enhancer import LineageEnhancer

def test_extract_dml_operations():
    extractor = DatabricksLineageExtractor()
    sql = "UPDATE users SET name = 'test' WHERE id = 1"
    ops = extractor.extract_operations(sql)
    assert 'UPDATE' in ops['dml']

def test_extract_merge_operations():
    extractor = DatabricksLineageExtractor()
    sql = "MERGE INTO target USING source ON target.id = source.id"
    ops = extractor.extract_operations(sql)
    assert len(ops['merge']) > 0
    assert 'target' in ops['merge'][0]

def test_extract_file_writes():
    extractor = DatabricksLineageExtractor()
    sql = "COPY INTO 's3://bucket/path' FROM table"
    ops = extractor.extract_operations(sql)
    assert len(ops['file_write']) > 0
    assert 's3://bucket/path' in ops['file_write'][0]

def test_column_lineage_extraction():
    extractor = DatabricksLineageExtractor()
    sql = "SELECT col1, col2 FROM table"
    lineage = extractor.extract_column_lineage(sql)
    assert 'col1' in lineage or 'col2' in lineage

def test_detect_python_udfs():
    extractor = DatabricksLineageExtractor()
    plan = "PythonUDF[func_name, arg1, arg2]"
    udfs = extractor.detect_python_udfs(plan)
    assert len(udfs) == 1
    assert udfs[0]['type'] == 'PythonUDF'

def test_lineage_enhancer_process_query():
    enhancer = LineageEnhancer()
    sql = "SELECT a, b FROM table WHERE c = 1"
    graph = enhancer.process_query(sql)
    assert isinstance(graph, nx.DiGraph)
    assert len(graph.nodes()) > 0

def test_lineage_enhancer_with_query_plan():
    enhancer = LineageEnhancer()
    sql = "SELECT transform(col) FROM table"
    plan = "PythonUDF[transform, col]"
    graph = enhancer.process_query(sql, plan)
    assert any('udf' in node for node in graph.nodes())

def test_get_lineage_for_column():
    enhancer = LineageEnhancer()
    enhancer.lineage_graph.add_edge('source_col', 'target_col')
    ancestors = enhancer.get_lineage_for_column('target_col')
    assert 'source_col' in ancestors

def test_export_lineage():
    enhancer = LineageEnhancer()
    enhancer.lineage_graph.add_node('col1', node_type='column')
    export = enhancer.export_lineage()
    assert 'nodes' in export
    assert 'edges' in export
    assert len(export['nodes']) == 1
