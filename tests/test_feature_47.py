import pytest
from tracepipe.parsers.spark_lineage import SparkLineageParser

def test_complex_udf_tracking():
    code = '''
def transform_data(value):
    return value * 2

transform_udf = udf(transform_data)
result_df = source_df.withColumn("doubled", transform_udf(col("amount")))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    assert 'transform_data' in result['udfs']
    assert result['udfs']['transform_data']['type'] == 'udf'

def test_column_lineage_with_qualified_names():
    code = '''
input_table = spark.read.table("source")
output_table = input_table.select("id", "name")
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    lineage = result['column_lineage']
    assert "output_table.id" in lineage
    assert "output_table.name" in lineage
    assert lineage["output_table.id"] == ["input_table.id"]
    assert lineage["output_table.name"] == ["input_table.name"]

def test_withcolumn_dependencies():
    code = '''
df_source = spark.read.table("data")
df_result = df_source.withColumn("total", col("price") + col("tax"))
'''
    parser = SparkLineageParser()
    result = parser.parse_code(code)
    lineage = result['column_lineage']
    assert "df_result.total" in lineage
    deps = lineage["df_result.total"]
    assert "df_source.price" in deps
    assert "df_source.tax" in deps
