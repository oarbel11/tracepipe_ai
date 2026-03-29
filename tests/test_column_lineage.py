import pytest
from scripts.lineage.column_lineage_extractor import ColumnLineageExtractor
from scripts.lineage.impact_analyzer import ImpactAnalyzer
from scripts.lineage.lineage_visualizer import LineageVisualizer


def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor()
    sql = "SELECT a.id, a.name AS customer_name, b.amount FROM users a JOIN orders b ON a.id = b.user_id"
    lineage = extractor.extract_from_sql(sql, "customer_orders")
    
    assert lineage["target_table"] == "customer_orders"
    assert "id" in lineage["columns"]
    assert "customer_name" in lineage["columns"]
    assert "amount" in lineage["columns"]
    assert len(lineage["sources"]) >= 2


def test_transformation_classification():
    extractor = ColumnLineageExtractor()
    sql = "SELECT SUM(amount) AS total, CONCAT(first_name, last_name) AS full_name FROM sales"
    lineage = extractor.extract_from_sql(sql, "summary")
    
    assert lineage["columns"]["total"]["transformation_type"] == "aggregation"
    assert lineage["columns"]["full_name"]["transformation_type"] == "string_manipulation"


def test_impact_analyzer():
    analyzer = ImpactAnalyzer()
    
    lineage1 = {
        "target_table": "table_b",
        "columns": {
            "col_b": {
                "source_columns": ["table_a.col_a"],
                "transformation_type": "direct"
            }
        }
    }
    
    lineage2 = {
        "target_table": "table_c",
        "columns": {
            "col_c": {
                "source_columns": ["table_b.col_b"],
                "transformation_type": "calculation"
            }
        }
    }
    
    analyzer.add_lineage("table_b", lineage1)
    analyzer.add_lineage("table_c", lineage2)
    
    impact = analyzer.analyze_column_impact("table_a", "col_a")
    
    assert "table_b" in impact["affected_tables"]
    assert "table_c" in impact["affected_tables"]
    assert impact["impact_depth"] >= 1


def test_lineage_visualizer():
    visualizer = LineageVisualizer()
    
    lineage = {
        "target_table": "result",
        "columns": {
            "output_col": {
                "source_columns": ["source.input_col"],
                "transformation_type": "direct",
                "expression": "source.input_col"
            }
        }
    }
    
    graph = visualizer.create_graph(lineage)
    
    assert "nodes" in graph
    assert "edges" in graph
    assert len(graph["nodes"]) >= 2
    assert len(graph["edges"]) >= 1
