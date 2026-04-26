import pytest
from tracepipe.parsers.spark_lineage import SparkLineageParser

def test_column_lineage_chain():
    code = '''
input_df = spark.read.table("customers")
output = input_df.select("customer_name", "customer_id")
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    lineage = result.get('column_lineage', {})
    assert "output.customer_name" in lineage
    assert "output.customer_id" in lineage
    assert lineage["output.customer_name"] == ["input_df.customer_name"]
    assert lineage["output.customer_id"] == ["input_df.customer_id"]

def test_withcolumn_lineage():
    code = '''
source = spark.read.table("data")
target = source.withColumn("full_name", col("first_name"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    lineage = result.get('column_lineage', {})
    assert "target.full_name" in lineage
    assert "source.first_name" in lineage["target.full_name"]

def test_udf_in_transformation():
    code = '''
def upper_case(s):
    return s.upper()

upper_udf = udf(upper_case)
df1 = spark.read.table("source")
df2 = df1.withColumn("upper_name", upper_udf(col("name")))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'upper_case' in result['udfs']
    assert 'column_lineage' in result
