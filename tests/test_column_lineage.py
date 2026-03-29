import pytest
from scripts.lineage.column_lineage_extractor import ColumnLineageExtractor
from scripts.lineage.column_impact_analyzer import ColumnImpactAnalyzer
from scripts.lineage.lineage_visualizer import LineageVisualizer
from unittest.mock import Mock, MagicMock


@pytest.fixture
def mock_workspace_client():
    return Mock()


@pytest.fixture
def extractor(mock_workspace_client):
    return ColumnLineageExtractor(mock_workspace_client)


@pytest.fixture
def analyzer(mock_workspace_client, extractor):
    return ColumnImpactAnalyzer(mock_workspace_client, extractor)


@pytest.fixture
def visualizer(extractor, analyzer):
    return LineageVisualizer(extractor, analyzer)


def test_extract_from_sql(extractor):
    sql = "SELECT col1, col2 AS new_col FROM table1"
    result = extractor.extract_from_sql(sql, "table1")
    
    assert isinstance(result, dict)
    assert "NEW_COL" in result or "COL2" in result


def test_extract_from_dataframe(extractor):
    df_code = 'df.select("col1", "col2").withColumn("col3", col("col1") + 1)'
    result = extractor.extract_from_dataframe(df_code)
    
    assert isinstance(result, dict)
    assert "col3" in result
    assert "col1" in result["col3"]


def test_get_column_lineage(extractor):
    result = extractor.get_column_lineage("test_table", "test_column")
    
    assert "column" in result
    assert result["column"] == "test_column"
    assert "table" in result
    assert "upstream" in result


def test_analyze_column_change(analyzer):
    result = analyzer.analyze_column_change("test_table", "test_column")
    
    assert "affected_tables" in result
    assert "risk_level" in result
    assert result["risk_level"] in ["LOW", "MEDIUM", "HIGH"]


def test_get_impact_report(analyzer):
    report = analyzer.get_impact_report("test_table", "test_column")
    
    assert isinstance(report, str)
    assert "Impact Analysis" in report
    assert "Risk Level" in report


def test_generate_lineage_graph(visualizer):
    graph = visualizer.generate_lineage_graph("test_table", "test_column")
    
    assert "nodes" in graph
    assert "edges" in graph
    assert isinstance(graph["nodes"], list)
    assert isinstance(graph["edges"], list)


def test_generate_impact_graph(visualizer):
    graph = visualizer.generate_impact_graph("test_table", "test_column")
    
    assert "nodes" in graph
    assert "edges" in graph
    assert "risk" in graph


def test_get_interactive_html(visualizer):
    html = visualizer.get_interactive_html("test_table", "test_column")
    
    assert isinstance(html, str)
    assert "<html>" in html
    assert "Column Lineage" in html
