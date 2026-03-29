import pytest
from src.column_lineage import ColumnLineageExtractor
from src.impact_analyzer import ImpactAnalyzer
from src.lineage_visualizer import LineageVisualizer

def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor()
    sql = "SELECT first_name, last_name, age FROM users"
    lineage = extractor.extract_lineage(sql, "users_view")
    assert lineage["target_table"] == "users_view"
    assert "first_name" in lineage["columns"]
    assert lineage["columns"]["first_name"]["transformation_type"] == "direct"

def test_transformation_classification():
    extractor = ColumnLineageExtractor()
    sql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users"
    lineage = extractor.extract_lineage(sql, "users_view")
    assert "full_name" in lineage["columns"]
    assert lineage["columns"]["full_name"]["transformation_type"] == "string_manipulation"
    assert "first_name" in lineage["columns"]["full_name"]["source_columns"]
    assert "last_name" in lineage["columns"]["full_name"]["source_columns"]

def test_impact_analyzer():
    analyzer = ImpactAnalyzer()
    lineage_data = [{
        "target_table": "table_b",
        "columns": {
            "col_x": {"source_columns": ["col_a"], "transformation_type": "direct"}
        }
    }]
    analyzer.build_graph(lineage_data)
    impact = analyzer.analyze_impact("table_a", "col_a")
    assert impact["source"]["table"] == "table_a"
    assert len(impact["downstream_impact"]) > 0

def test_lineage_visualizer():
    visualizer = LineageVisualizer()
    lineage = {
        "target_table": "users_view",
        "columns": {
            "full_name": {
                "source_columns": ["first_name", "last_name"],
                "transformation_type": "string_manipulation"
            }
        }
    }
    graph = visualizer.generate_graph(lineage)
    assert len(graph["nodes"]) == 3
    assert len(graph["edges"]) == 2
    ascii_output = visualizer.render_ascii(graph)
    assert "first_name" in ascii_output
