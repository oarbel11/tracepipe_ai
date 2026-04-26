import pytest
from tracepipe.parsers.spark_lineage import SparkLineageParser

def test_simple_select():
    code = '''
df1 = spark.read.table("source")
df2 = df1.select("col1", "col2")
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'transformations' in result
    assert any(t['operation'] == 'select' for t in result['transformations'])

def test_with_column():
    code = '''
df1 = spark.read.table("source")
df2 = df1.withColumn("new_col", col("old_col"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert any(t['operation'] == 'withColumn' for t in result['transformations'])

def test_udf_detection():
    code = '''
def my_func(x):
    return x.upper()

my_udf = udf(my_func)
df2 = df1.withColumn("new_col", my_udf(col("old_col")))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'udfs' in result
    assert 'my_func' in result['udfs']

def test_chained_operations():
    code = '''
df1 = spark.read.table("source")
df2 = df1.select("col1").withColumn("col2", col("col1"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert len(result['transformations']) >= 2
