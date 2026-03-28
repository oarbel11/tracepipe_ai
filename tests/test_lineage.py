"""Tests for column-level lineage extraction."""

import pytest
from scripts.lineage import SparkColumnParser, ColumnLineageGraph, NotebookLineageAnalyzer


def test_spark_column_parser_python():
    """Test parsing Python Spark code."""
    parser = SparkColumnParser()
    code = 'df.select("col1", "col2")'
    mappings = parser.parse_python_code(code)
    assert isinstance(mappings, list)


def test_spark_column_parser_scala():
    """Test parsing Scala Spark code."""
    parser = SparkColumnParser()
    code = 'df.select("col1").withColumn("col2", col("col1"))'
    mappings = parser.parse_scala_code(code)
    assert isinstance(mappings, list)
    assert len(mappings) >= 1


def test_column_lineage_graph():
    """Test building lineage graph."""
    graph = ColumnLineageGraph()
    source_col = graph.add_column("source_table", "col1")
    target_col = graph.add_column("target_table", "col2")
    graph.add_transformation(source_col, target_col, "select")
    
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
    
    upstream = graph.get_upstream_columns(target_col)
    assert source_col in upstream


def test_notebook_analyzer():
    """Test notebook analysis."""
    analyzer = NotebookLineageAnalyzer()
    cells = [
        {"cell_type": "code", "source": 'df.select("col1")', "language": "python"}
    ]
    result = analyzer.analyze_notebook("test.ipynb", cells)
    
    assert "notebook_path" in result
    assert "total_transformations" in result
    assert "transformations" in result


def test_lineage_graph_build_from_mappings():
    """Test building graph from mappings."""
    graph = ColumnLineageGraph()
    mappings = [
        {"operation": "select", "target": "col1", "sources": ["col1"], "expression": "col1"}
    ]
    graph.build_from_mappings(mappings, "source", "target")
    
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1


def test_lineage_graph_to_dict():
    """Test exporting graph to dictionary."""
    graph = ColumnLineageGraph()
    graph.add_column("table1", "col1")
    result = graph.to_dict()
    
    assert "nodes" in result
    assert "edges" in result
    assert isinstance(result["nodes"], dict)


def test_notebook_analyzer_impact_analysis():
    """Test column impact analysis."""
    analyzer = NotebookLineageAnalyzer()
    analyzer.graph.add_column("table1", "col1")
    analyzer.graph.add_column("table2", "col2")
    analyzer.graph.add_transformation("table1.col1", "table2.col2", "select")
    
    impact = analyzer.get_column_impact("table2.col2")
    assert "upstream_dependencies" in impact
    assert "downstream_impacts" in impact
