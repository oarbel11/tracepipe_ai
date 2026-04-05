"""Test column lineage tracking functionality."""
import pytest
import os
import tempfile
import shutil
from scripts.lineage.lineage_parser import ColumnLineageParser, ColumnLineage
from scripts.lineage.lineage_tracker import LineageTracker
from scripts.lineage.udf_analyzer import UDFAnalyzer


def test_parse_simple_select():
    parser = ColumnLineageParser()
    sql = "SELECT col1, col2 AS renamed_col FROM table1"
    lineages = parser.parse_sql(sql)
    assert len(lineages) == 2
    assert lineages[0].target_column == "col1"
    assert lineages[1].target_column == "renamed_col"


def test_parse_transformation():
    parser = ColumnLineageParser()
    sql = "SELECT col1 + col2 AS sum_col FROM table1"
    lineages = parser.parse_sql(sql)
    assert len(lineages) == 1
    assert lineages[0].target_column == "sum_col"
    assert "col1" in lineages[0].source_columns
    assert "col2" in lineages[0].source_columns


def test_parse_dataframe_code():
    parser = ColumnLineageParser()
    code = 'df.withColumn("new_col", col("old_col") * 2)'
    lineages = parser.parse_dataframe_code(code)
    assert len(lineages) == 1
    assert lineages[0].target_column == "new_col"


def test_lineage_tracker():
    with tempfile.TemporaryDirectory() as tmpdir:
        tracker = LineageTracker(storage_path=tmpdir)
        lineage = ColumnLineage(target_column="col1", source_columns=["src1"])
        tracker.add_lineage("table1", [lineage])
        result = tracker.get_lineage("table1")
        assert len(result) == 1
        assert result[0]["target_column"] == "col1"


def test_manual_lineage_override():
    with tempfile.TemporaryDirectory() as tmpdir:
        tracker = LineageTracker(storage_path=tmpdir)
        tracker.add_manual_lineage("table1", "col1", ["src1", "src2"], "custom_transform")
        result = tracker.get_lineage("table1")
        assert len(result) == 1
        assert result[0]["manual_override"] is True


def test_udf_analyzer_python():
    analyzer = UDFAnalyzer()
    udf_code = "def my_udf(col1, col2):\n    return col1 + col2"
    params = analyzer.analyze_python_udf(udf_code)
    assert "col1" in params
    assert "col2" in params


def test_udf_lineage_creation():
    analyzer = UDFAnalyzer()
    udf_code = "def transform(input_col):\n    return input_col * 2"
    lineage = analyzer.create_udf_lineage("transform", udf_code, "output_col")
    assert lineage.target_column == "output_col"
    assert "input_col" in lineage.source_columns
