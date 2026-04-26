import pytest
from src.spark_lineage_parser import SparkLineageParser
from src.udf_analyzer import UDFAnalyzer
from src.column_lineage_tracker import ColumnLineageTracker

def test_simple_udf_lineage():
    """Test basic UDF column lineage tracking"""
    parser = SparkLineageParser()
    code = '''
from pyspark.sql.functions import udf

@udf
def upper_case(name):
    return name.upper()

df = spark.table("customers")
result = df.withColumn("upper_name", upper_case(col("name")))
'''
    result = parser.parse_code(code)
    assert 'udfs' in result
    assert len(result['udfs']) > 0

def test_column_tracking_with_tables():
    """Test column lineage with table context"""
    tracker = ColumnLineageTracker()
    tracker.track_transformation(
        output_col="customer_name",
        input_cols=["first_name", "last_name"],
        output_table="output",
        input_table="input"
    )
    lineage = tracker.get_full_lineage()
    assert "output.customer_name" in lineage
    assert "input.first_name" in lineage["output.customer_name"]
    assert "input.last_name" in lineage["output.customer_name"]

def test_udf_dependency_extraction():
    """Test UDF dependency analysis"""
    analyzer = UDFAnalyzer()
    udf_code = '''
def calculate_total(price, quantity):
    return price * quantity
'''
    result = analyzer.analyze_udf(udf_code, "calculate_total")
    assert "calculate_total" in result
    deps = result["calculate_total"]
    assert "price" in deps or "quantity" in deps

def test_transitive_lineage():
    """Test transitive column dependencies"""
    tracker = ColumnLineageTracker()
    tracker.add_lineage("col_c", ["col_b"])
    tracker.add_lineage("col_b", ["col_a"])
    transitive = tracker.get_transitive_lineage("col_c")
    assert "col_a" in transitive
    assert "col_b" in transitive
