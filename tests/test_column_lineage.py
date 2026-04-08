import pytest
from src.column_lineage_extractor import ColumnLineageExtractor
from src.transformation_classifier import TransformationClassifier
from src.impact_analyzer import ImpactAnalyzer
from src.lineage_visualizer import LineageVisualizer

def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor()
    sql = "SELECT first_name, last_name, CONCAT(first_name, ' ', last_name) AS full_name FROM users"
    lineage = extractor.extract_lineage(sql)
    
    assert "first_name" in lineage["columns"]
    assert "full_name" in lineage["columns"]
    assert lineage["columns"]["first_name"]["transformation_type"] == "passthrough"
    assert lineage["columns"]["full_name"]["transformation_type"] == "expression"

def test_transformation_classification():
    classifier = TransformationClassifier()
    
    passthrough = {"transformation_type": "passthrough"}
    assert classifier.classify(passthrough) == "passthrough"
    
    concat = {"transformation_type": "expression", "expression": "CONCAT(first_name, last_name)"}
    assert classifier.classify(concat) == "concatenation"

def test_impact_analyzer():
    analyzer = ImpactAnalyzer()
    lineage = {
        "columns": {
            "full_name": {"source_columns": ["first_name", "last_name"]}
        }
    }
    analyzer.add_lineage("users", lineage)
    impact = analyzer.analyze_impact("users", "first_name")
    assert "tables" in impact
    assert "columns" in impact

def test_lineage_visualizer():
    visualizer = LineageVisualizer()
    lineage = {
        "columns": {
            "full_name": {"source_columns": ["first_name", "last_name"], "transformation_type": "expression"}
        }
    }
    graph = visualizer.generate_graph(lineage)
    assert "nodes" in graph
    assert "edges" in graph
    assert len(graph["nodes"]) >= 3
