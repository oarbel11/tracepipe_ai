"""Tests for Feature #47: Enhanced UDF and complex transformation support."""
import pytest
from tracepipe_ai.parsers.spark_parser import SparkLineageParser


def test_udf_registration():
    """Test UDF registration and dependency extraction."""
    parser = SparkLineageParser()

    def my_udf(row):
        return row["col1"] + row["col2"]

    parser.register_udf("my_udf", my_udf, ["col1", "col2"])
    assert "my_udf" in parser.udf_registry
    assert parser.udf_lineage["my_udf"] == ["col1", "col2"]


def test_udf_auto_dependency_extraction():
    """Test automatic extraction of UDF dependencies from code."""
    parser = SparkLineageParser()

    def compute_udf(row):
        return row["amount"] * row["rate"]

    parser.register_udf("compute_udf", compute_udf)
    deps = parser.udf_lineage["compute_udf"]
    assert "amount" in deps
    assert "rate" in deps


def test_transformation_parsing():
    """Test parsing of complex transformations."""
    parser = SparkLineageParser()
    lineage = parser.parse_transformation(
        "withColumn('total', col1 + col2)", ["col1", "col2"])
    assert "total" in lineage
    assert set(lineage["total"]) <= {"col1", "col2"}


def test_lineage_tracking():
    """Test end-to-end lineage tracking."""
    parser = SparkLineageParser()

    def my_udf(row):
        return row["a"] + row["b"]

    parser.register_udf("my_udf", my_udf, ["a", "b"])

    operations = [
        {"type": "udf", "name": "my_udf", "output_col": "result"},
        {"type": "transform", "operation": "withColumn('derived', result)",
         "columns": ["result"]}
    ]

    lineage = parser.track_lineage(operations)
    assert "result" in lineage
    assert lineage["result"] == {"a", "b"}
