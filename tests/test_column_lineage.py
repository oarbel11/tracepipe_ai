import pytest
from tracepipe_ai.column_lineage import (
    ColumnLineageExtractor, ImpactAnalyzer, LineageVisualizer
)

def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor()
    sql = """SELECT first_name, last_name, 
             CONCAT(first_name, ' ', last_name) AS full_name 
             FROM users"""
    lineage = extractor.extract_lineage("users_transformed", sql)
    assert "first_name" in lineage["columns"]
    assert lineage["columns"]["full_name"]["transformation_type"] == "concat"
    assert "first_name" in lineage["columns"]["full_name"]["source_columns"]

def test_transformation_classification():
    extractor = ColumnLineageExtractor()
    assert extractor._classify_transformation(
        "CONCAT(first_name, last_name)", ["first_name", "last_name"]
    ) == "concat"
    assert extractor._classify_transformation(
        "CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END", ["age"]
    ) == "conditional"
    assert extractor._classify_transformation(
        "first_name", ["first_name"]
    ) == "direct"

def test_impact_analyzer():
    analyzer = ImpactAnalyzer()
    lineage_data = [
        {"table": "t1", "columns": {"col_a": {"source_columns": []}}},
        {"table": "t2", "columns": {"col_b": {"source_columns": ["col_a"]}}}
    ]
    analyzer.build_graph(lineage_data)
    impact = analyzer.analyze_impact("t1", "col_a")
    assert "t2.col_b" in impact["impacted_columns"]

def test_lineage_visualizer():
    visualizer = LineageVisualizer()
    lineage = {
        "table": "users",
        "columns": {
            "full_name": {
                "source_columns": ["first_name", "last_name"],
                "transformation_type": "concat"
            }
        }
    }
    graph = visualizer.generate_graph(lineage)
    assert len(graph["nodes"]) > 0
    assert len(graph["edges"]) > 0
