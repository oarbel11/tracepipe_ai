"""Tests for Databricks lineage extraction."""
import pytest
from tracepipe_ai.databricks_lineage_extractor import DatabricksLineageExtractor
from tracepipe_ai.lineage_enhancer import LineageEnhancer


def test_extract_udfs():
    """Test UDF extraction from queries."""
    extractor = DatabricksLineageExtractor()
    query = "SELECT my_udf(col1), another_udf(col2) FROM table1"
    lineage = extractor.extract_lineage(query)
    assert "my_udf" in lineage["udfs"]
    assert "another_udf" in lineage["udfs"]


def test_extract_dml_operations():
    """Test DML operation detection."""
    extractor = DatabricksLineageExtractor()
    queries = [
        "UPDATE table1 SET col1 = 'value'",
        "DELETE FROM table1 WHERE col1 = 'value'",
        "INSERT INTO table1 VALUES (1, 2, 3)"
    ]
    for query in queries:
        lineage = extractor.extract_lineage(query)
        assert len(lineage["dml_operations"]) > 0


def test_extract_file_operations():
    """Test file path extraction."""
    extractor = DatabricksLineageExtractor()
    query = "CREATE TABLE t1 LOCATION 's3://bucket/path'"
    lineage = extractor.extract_lineage(query)
    assert "s3://bucket/path" in lineage["file_operations"]


def test_extract_columns():
    """Test column extraction from SELECT."""
    extractor = DatabricksLineageExtractor()
    query = "SELECT col1, col2, col3 FROM table1"
    lineage = extractor.extract_lineage(query)
    assert len(lineage["columns"]) == 3


def test_extract_tables():
    """Test table extraction."""
    extractor = DatabricksLineageExtractor()
    query = "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
    lineage = extractor.extract_lineage(query)
    assert "table1" in lineage["tables"]
    assert "table2" in lineage["tables"]


def test_lineage_enhancer():
    """Test lineage graph building."""
    enhancer = LineageEnhancer()
    query = "SELECT my_udf(col1) FROM table1"
    result = enhancer.build_lineage(query)
    assert "lineage_data" in result
    assert "graph" in result
    assert len(result["graph"]["nodes"]) > 0


def test_merge_statement():
    """Test MERGE statement detection."""
    extractor = DatabricksLineageExtractor()
    query = "MERGE INTO target USING source ON target.id = source.id"
    lineage = extractor.extract_lineage(query)
    assert "MERGE" in lineage["dml_operations"]
