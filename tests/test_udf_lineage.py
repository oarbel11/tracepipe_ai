import pytest
from scripts.udf_lineage_tracker import UDFLineageTracker, UDFMetadata
from scripts.spark_lineage_parser import SparkLineageParser
from scripts.advanced_transform_tracker import AdvancedTransformTracker

SAMPLE_CODE_WITH_UDF = '''
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def concat_names(first, last):
    return f"{first} {last}"

df = spark.table("users")
df = df.withColumn("full_name", concat_names(col("first_name"), col("last_name")))
df = df.withColumn("upper_name", col("full_name").upper())
'''

def test_udf_extraction():
    tracker = UDFLineageTracker()
    udfs = tracker.extract_udf_from_code(SAMPLE_CODE_WITH_UDF)
    assert len(udfs) > 0
    assert udfs[0].name == "concat_names"

def test_udf_lineage_tracking():
    tracker = UDFLineageTracker()
    tracker.extract_udf_lineage(code=SAMPLE_CODE_WITH_UDF)
    assert "concat_names" in tracker.udfs
    assert "full_name" in tracker.column_lineage
    deps = tracker.get_column_dependencies("users", "full_name")
    assert "source_columns" in deps

def test_spark_lineage_parser_with_udf():
    parser = SparkLineageParser()
    result = parser.parse_spark_code(SAMPLE_CODE_WITH_UDF)
    assert "tables" in result
    assert "users" in result["tables"]
    assert "udfs" in result
    assert "concat_names" in result["udfs"]

def test_complex_transformations():
    code = '''
    df.explode("tags").groupBy("category").pivot("status").agg(count("*"))
    '''
    tracker = AdvancedTransformTracker()
    transformations = tracker.track_transformation(code)
    assert len(transformations) >= 3
    ops = [t.operation for t in transformations]
    assert "explode" in ops
    assert "groupBy" in ops
    assert "pivot" in ops

def test_column_lineage_chain():
    parser = SparkLineageParser()
    code = '''
    df = spark.table("source")
    df = df.select(col("id"), col("name") as "customer_name")
    '''
    result = parser.parse_spark_code(code)
    lineage = result.get("column_lineage", {})
    assert "output.customer_name" in lineage

def test_nested_udf_dependencies():
    code = '''
    @udf(returnType=StringType())
    def process_data(value):
        return value.strip().lower()
    
    df.withColumn("clean_name", process_data(col("raw_name")))
    '''
    tracker = UDFLineageTracker()
    tracker.extract_udf_lineage(code=code)
    deps = tracker.get_column_dependencies("table", "clean_name")
    assert isinstance(deps["source_columns"], list)
