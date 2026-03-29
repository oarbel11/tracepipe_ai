import pytest
from tracepipe_ai.column_lineage import ColumnLineageExtractor
from tracepipe_ai.transformation_classifier import TransformationClassifier
from tracepipe_ai.impact_analyzer import ImpactAnalyzer
from tracepipe_ai.lineage_visualizer import LineageVisualizer

def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor("main")
    sql = "SELECT first_name, last_name, CONCAT(first_name, ' ', last_name) AS full_name FROM users"
    lineage = extractor.extract_lineage(sql, "users_processed")
    
    assert "first_name" in lineage["columns"]
    assert "last_name" in lineage["columns"]
    assert "full_name" in lineage["columns"]
    assert lineage["columns"]["full_name"]["source_columns"] == ["first_name", "last_name"]
    assert lineage["columns"]["full_name"]["transformation_type"] == "concat"

def test_transformation_classification():
    classifier = TransformationClassifier()
    result = classifier.classify("concat")
    assert result["category"] == "string"
    assert result["complexity_score"] == 3
    
    result = classifier.classify("case")
    assert result["category"] == "conditional"
    assert result["complexity_score"] == 5

def test_impact_analyzer():
    analyzer = ImpactAnalyzer()
    lineage_data = {
        "users_processed": {
            "source_tables": ["users"],
            "columns": {
                "full_name": {
                    "source_columns": ["first_name", "last_name"],
                    "transformation_type": "concat"
                }
            }
        },
        "user_reports": {
            "source_tables": ["users_processed"],
            "columns": {
                "name": {
                    "source_columns": ["full_name"],
                    "transformation_type": "passthrough"
                }
            }
        }
    }
    
    impact = analyzer.analyze_column_impact("first_name", "users", lineage_data)
    assert impact["column"] == "first_name"
    assert len(impact["downstream_dependencies"]) >= 1
    assert impact["impact_score"] > 0

def test_lineage_visualizer():
    visualizer = LineageVisualizer()
    lineage_data = {
        "target_table": "users_processed",
        "source_tables": ["users"],
        "columns": {
            "full_name": {
                "source_columns": ["first_name", "last_name"],
                "transformation_type": "concat"
            }
        }
    }
    
    graph = visualizer.generate_graph(lineage_data, format="json")
    assert "nodes" in graph
    assert "edges" in graph
    assert len(graph["nodes"]) > 0
    assert len(graph["edges"]) > 0
