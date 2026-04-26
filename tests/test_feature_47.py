"""Tests for Feature #47: Advanced Spark UDFs and Complex Transformations."""
import pytest
from tracepipe_ai.parsers import SparkLineageParser


def test_basic_udf_lineage():
    """Test basic UDF with single column dependency."""
    code = '''
def upper_udf(s):
    return s.upper()

df.withColumn("upper_name", upper_udf(col("name")))
'''
    parser = SparkLineageParser()
    lineage = parser.parse_code(code)
    assert "upper_name" in lineage
    assert "name" in lineage["upper_name"]


def test_multi_column_udf():
    """Test UDF with multiple column dependencies."""
    code = '''
def concat_udf(row):
    return row["first"] + " " + row["last"]

df.withColumn("full_name", concat_udf(struct(col("first"), col("last"))))
'''
    parser = SparkLineageParser()
    lineage = parser.parse_code(code)
    assert "full_name" in lineage
    assert "first" in lineage["full_name"]
    assert "last" in lineage["full_name"]


def test_nested_transformations():
    """Test nested DataFrame transformations."""
    code = '''
df.withColumn("temp", col("a") + col("b")).withColumn("result", col("temp") * 2)
'''
    parser = SparkLineageParser()
    lineage = parser.parse_code(code)
    assert "temp" in lineage
    assert set(lineage["temp"]) == {"a", "b"}
    assert "result" in lineage
    assert "temp" in lineage["result"]


def test_complex_select():
    """Test complex select with multiple columns."""
    code = '''
df.select(col("id"), col("name").alias("customer_name"), col("amount"))
'''
    parser = SparkLineageParser()
    lineage = parser.parse_code(code)
    assert "id" in lineage
    assert lineage["id"] == ["id"]


def test_empty_code():
    """Test parsing empty code."""
    parser = SparkLineageParser()
    lineage = parser.parse_code("")
    assert lineage == {}


def test_no_lineage():
    """Test code with no lineage operations."""
    code = '''
x = 5
y = 10
z = x + y
'''
    parser = SparkLineageParser()
    lineage = parser.parse_code(code)
    assert lineage == {}
