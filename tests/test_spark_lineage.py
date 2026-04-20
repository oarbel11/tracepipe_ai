"""Tests for Spark lineage parser."""

from tracepipe.lineage.spark_lineage_parser import SparkLineageParser


def test_parse_simple_withcolumn():
    """Test parsing simple withColumn operation."""
    code = '''
from pyspark.sql.functions import col
df2 = df.withColumn("new_col", col("old_col"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert "lineage" in result
    assert len(result["lineage"]) > 0
    assert result["lineage"][0]["target"] == "new_col"
    assert "old_col" in result["lineage"][0]["sources"]


def test_parse_udf():
    """Test parsing UDF definitions."""
    code = '''
from pyspark.sql.functions import udf

@udf
def my_transform(x):
    return x.upper()
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert "udfs" in result
    assert len(result["udfs"]) > 0
    assert result["udfs"][0]["name"] == "my_transform"


def test_get_column_dependencies():
    """Test getting column dependencies."""
    code = '''
from pyspark.sql.functions import col
df2 = df.withColumn("result", col("input_col"))
'''
    parser = SparkLineageParser()
    deps = parser.get_column_dependencies(code, "result")
    assert "input_col" in deps
