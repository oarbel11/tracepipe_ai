import pytest
from scripts.spark_lineage_parser import SparkLineageParser
from scripts.lineage_extractor import LineageExtractor, ColumnNode
from scripts.spark_analyzer import SparkAnalyzer

def test_parse_simple_withcolumn():
    code = '''
from pyspark.sql import functions as F
df = df.withColumn("new_col", F.col("old_col") + 1)
'''
    parser = SparkLineageParser()
    parser.parse_code(code)
    assert len(parser.operations) == 1
    assert parser.operations[0].op_type == "withColumn"
    assert "old_col" in parser.operations[0].inputs
    assert "new_col" in parser.operations[0].outputs

def test_parse_udf_decorator():
    code = '''
from pyspark.sql.functions import udf

@udf
def my_transform(x, y):
    return x + y
'''
    parser = SparkLineageParser()
    parser.parse_code(code)
    assert len(parser.udfs) == 1
    assert "my_transform" in parser.udfs
    assert parser.udfs["my_transform"].params == ["x", "y"]

def test_parse_select_operation():
    code = '''
df = df.select("col1", "col2", "col3")
'''
    parser = SparkLineageParser()
    parser.parse_code(code)
    assert len(parser.operations) >= 1
    select_ops = [op for op in parser.operations if op.op_type == "select"]
    assert len(select_ops) > 0

def test_lineage_extractor():
    extractor = LineageExtractor()
    extractor.add_transformation("table_a", "col1", "table_b", "col2", "transform")
    upstream = extractor.get_upstream_columns("table_b", "col2")
    assert len(upstream) == 1
    assert upstream[0].table == "table_a"
    assert upstream[0].column == "col1"

def test_lineage_path():
    extractor = LineageExtractor()
    extractor.add_transformation("t1", "a", "t2", "b")
    extractor.add_transformation("t2", "b", "t3", "c")
    paths = extractor.get_lineage_path("t1", "a", "t3", "c")
    assert len(paths) > 0
    assert len(paths[0]) == 3

def test_spark_analyzer():
    code = '''
df = df.withColumn("total", F.col("price") * F.col("quantity"))
'''
    analyzer = SparkAnalyzer()
    result = analyzer.analyze_code(code)
    assert result['operations_count'] >= 1
    assert len(result['column_lineage']) >= 1

def test_column_impact():
    extractor = LineageExtractor()
    extractor.add_transformation("source", "col_a", "middle", "col_b")
    extractor.add_transformation("middle", "col_b", "target", "col_c")
    impact = extractor.get_column_impact("middle", "col_b")
    assert impact['upstream_count'] == 1
    assert impact['downstream_count'] == 1
