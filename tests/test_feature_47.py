"""Integration tests for Feature #47."""

import pytest
from tracepipe.parsers.spark_lineage import SparkLineageParser


def test_complex_udf_lineage():
    """Test UDF with multiple inputs and complex transformations."""
    parser = SparkLineageParser()
    parser.register_udf("complex_transform", ["col_a", "col_b", "col_c"],
                        "result")
    
    query = "SELECT complex_transform(col_a, col_b, col_c) AS output FROM table"
    lineage = parser.parse_query(query)
    
    assert "output" in lineage
    assert {"col_a", "col_b", "col_c"} <= lineage["output"]


def test_nested_udf_calls():
    """Test nested UDF calls in transformations."""
    parser = SparkLineageParser()
    parser.register_udf("udf1", ["x"], "temp")
    parser.register_udf("udf2", ["temp", "y"], "final")
    
    query = "SELECT udf2(udf1(x), y) AS result FROM table"
    lineage = parser.parse_query(query)
    
    assert "result" in lineage
    assert "x" in lineage["result"]
    assert "y" in lineage["result"]


def test_mixed_udf_and_native_operations():
    """Test combining UDFs with native Spark operations."""
    parser = SparkLineageParser()
    parser.register_udf("custom_udf", ["a"], "transformed")
    
    query = "SELECT custom_udf(a) + b AS combined FROM table"
    lineage = parser.parse_query(query)
    
    assert "combined" in lineage
    assert "a" in lineage["combined"]
    assert "b" in lineage["combined"]
