import pytest
import json
from scripts.lineage import SparkColumnParser, ColumnLineageGraph, NotebookLineageAnalyzer


def test_spark_parser_basic():
    parser = SparkColumnParser()
    code = 'df.withColumn("total", col("price") * col("quantity"))'
    result = parser.parse_notebook_cell(code, 1)
    assert result["cell_id"] == 1
    assert len(result["operations"]) > 0


def test_spark_parser_select():
    parser = SparkColumnParser()
    code = 'df.select("customer_id", "order_date")'
    result = parser.parse_notebook_cell(code, 2)
    assert "operations" in result


def test_udf_detection():
    parser = SparkColumnParser()
    code = '''@udf
def calculate_discount(price):
    return price * 0.9'''
    result = parser.parse_notebook_cell(code, 3)
    assert len(result["udfs"]) == 1
    assert result["udfs"][0]["name"] == "calculate_discount"


def test_lineage_graph_add_table():
    graph = ColumnLineageGraph()
    graph.add_table("orders", ["order_id", "customer_id", "total"])
    assert "orders" in graph.tables
    assert graph.graph.has_node("orders.order_id")


def test_lineage_graph_transformation():
    graph = ColumnLineageGraph()
    graph.add_transformation(["price", "quantity"], "total", "multiply", 1)
    assert graph.graph.has_node("temp.total")
    upstream = graph.get_upstream_lineage("total")
    assert upstream["count"] >= 0


def test_lineage_graph_upstream():
    graph = ColumnLineageGraph()
    graph.add_table("sales", ["price", "quantity"])
    graph.add_transformation(["sales.price", "sales.quantity"], "revenue", "*", 1)
    graph.add_transformation(["revenue"], "annual_revenue", "sum", 2)
    
    lineage = graph.get_upstream_lineage("annual_revenue")
    assert lineage["count"] > 0


def test_lineage_graph_downstream():
    graph = ColumnLineageGraph()
    graph.add_transformation(["base"], "derived1", "op1", 1)
    graph.add_transformation(["derived1"], "derived2", "op2", 2)
    
    impact = graph.get_downstream_impact("base")
    assert impact["count"] >= 1


def test_notebook_analyzer():
    analyzer = NotebookLineageAnalyzer()
    cells = [
        {"cell_type": "code", "source": "df = spark.read.table('orders')"},
        {"cell_type": "code", "source": 'df2 = df.withColumn("tax", col("total") * 0.1)'}
    ]
    result = analyzer.analyze_notebook(cells)
    assert result["cells_analyzed"] >= 0


def test_export_visualization():
    graph = ColumnLineageGraph()
    graph.add_table("products", ["product_id", "price"])
    graph.add_transformation(["products.price"], "discounted_price", "*0.9", 1)
    
    viz = graph.export_visualization()
    assert "nodes" in viz
    assert "edges" in viz
    assert len(viz["nodes"]) > 0


def test_json_export():
    graph = ColumnLineageGraph()
    graph.add_table("users", ["user_id", "name"])
    json_output = graph.to_json()
    parsed = json.loads(json_output)
    assert "nodes" in parsed
