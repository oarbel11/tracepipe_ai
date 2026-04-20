import pytest
from tracepipe.lineage.spark_lineage_parser import SparkLineageParser

@pytest.fixture
def parser():
    return SparkLineageParser()

def test_simple_select(parser):
    code = 'df.select("col1", "col2")'
    result = parser.parse_code(code)
    assert "col1" in result["columns"]
    assert "col2" in result["columns"]
    assert len(result["operations"]) > 0
    assert result["operations"][0]["type"] == "select"

def test_with_column(parser):
    code = 'df.withColumn("new_col", col("old_col"))'
    result = parser.parse_code(code)
    assert "new_col" in result["columns"]
    assert "old_col" in result["columns"]["new_col"]
    ops = [op for op in result["operations"] if op["type"] == "withColumn"]
    assert len(ops) > 0

def test_udf_detection(parser):
    code = '''@udf
def my_udf(x):
    return x + 1
df.withColumn("result", my_udf(col("input")))'''
    result = parser.parse_code(code)
    assert any(op["type"] == "udf" for op in result["operations"])

def test_chained_operations(parser):
    code = 'df.select("a", "b").withColumn("c", col("a") + col("b"))'
    result = parser.parse_code(code)
    assert "a" in result["columns"]
    assert "b" in result["columns"]
    assert "c" in result["columns"]
    assert len(result["operations"]) >= 2

def test_group_by(parser):
    code = 'df.groupBy("category").agg(sum("amount"))'
    result = parser.parse_code(code)
    assert any(op["type"] == "groupBy" for op in result["operations"])

def test_filter_operation(parser):
    code = 'df.filter(col("age") > 18)'
    result = parser.parse_code(code)
    assert any(op["type"] == "filter" for op in result["operations"])

def test_complex_transformation(parser):
    code = '''df.select("id", "name", "value").withColumn("double_value", col("value") * 2).filter(col("double_value") > 100)'''
    result = parser.parse_code(code)
    assert "double_value" in result["columns"]
    assert "value" in result["columns"]["double_value"]
    assert len(result["operations"]) >= 3

def test_empty_code(parser):
    result = parser.parse_code("")
    assert result["columns"] == {}
    assert result["operations"] == []

def test_invalid_syntax(parser):
    code = "df.select("
    result = parser.parse_code(code)
    assert result["columns"] == {}
    assert result["operations"] == []
