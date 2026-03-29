import pytest
from tracepipe_ai.column_lineage import (
    ColumnLineageExtractor, ImpactAnalyzer, LineageVisualizer
)

def test_column_lineage_extractor():
    extractor = ColumnLineageExtractor()
    sql = "SELECT customer_id, order_date FROM orders"
    lineage = extractor.extract_lineage(sql, "sales_summary")
    
    assert lineage["target_table"] == "sales_summary"
    assert "customer_id" in lineage["columns"]
    assert "order_date" in lineage["columns"]

def test_transformation_classification():
    extractor = ColumnLineageExtractor()
    sql = "SELECT CONCAT(first_name, last_name) AS full_name FROM users"
    lineage = extractor.extract_lineage(sql, "user_profiles")
    
    assert "full_name" in lineage["columns"]
    assert lineage["columns"]["full_name"]["transformation_type"] == "string_manipulation"
    assert "first_name" in lineage["columns"]["full_name"]["source_columns"]
    assert "last_name" in lineage["columns"]["full_name"]["source_columns"]

def test_impact_analyzer():
    analyzer = ImpactAnalyzer()
    extractor = ColumnLineageExtractor()
    
    sql1 = "SELECT customer_id, total FROM orders"
    lineage1 = extractor.extract_lineage(sql1, "order_summary")
    analyzer.add_lineage(lineage1)
    
    sql2 = "SELECT customer_id, SUM(total) AS revenue FROM order_summary"
    lineage2 = extractor.extract_lineage(sql2, "customer_revenue")
    analyzer.add_lineage(lineage2)
    
    impact = analyzer.analyze_impact("order_summary", "total")
    assert impact["total_impacted"] >= 1

def test_lineage_visualizer():
    extractor = ColumnLineageExtractor()
    visualizer = LineageVisualizer()
    
    sql = "SELECT customer_id, order_date FROM orders"
    lineage = extractor.extract_lineage(sql, "sales_summary")
    graph = visualizer.generate_graph(lineage)
    
    assert "nodes" in graph
    assert "edges" in graph
    assert len(graph["nodes"]) > 0
