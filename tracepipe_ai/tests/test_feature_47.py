"""Test Feature #47: Enhanced Spark UDF and Complex Transformation Support."""

import pytest
from tracepipe_ai.parsers.spark_lineage import SparkLineageParser


def test_udf_parsing():
    """Test UDF detection and parsing."""
    code = '''
@udf
def transform_value(col1, col2):
    return col1 + col2
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert "transform_value" in result["udfs"]
    assert result["udfs"]["transform_value"]["inputs"] == ["col1", "col2"]


def test_column_lineage_tracking():
    """Test column-level lineage with qualified names."""
    code = '''
df2 = df1.select("id", "name")
df3 = df2.withColumn("full_name", col("name"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    lineage = result["column_lineage"]
    assert "df2" in lineage
    assert any("id" in col for col in lineage.get("df2", []))


def test_complex_transformations():
    """Test complex DataFrame operations."""
    code = '''
@udf
def custom_calc(x, y):
    return x * y + 10

df_result = df_input.select("col_a", "col_b").withColumn("calc", custom_calc("col_a", "col_b"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert "custom_calc" in result["udfs"]
    assert "df_result" in result["column_lineage"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
