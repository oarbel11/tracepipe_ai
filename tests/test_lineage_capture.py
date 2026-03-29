"""Unit tests for lineage capture functionality."""

import pytest
from tracepipe_ai.lineage_capture import (
    UnmanagedCapture,
    UDFMapper,
    LineageTracker
)


class TestUnmanagedCapture:
    """Test cases for UnmanagedCapture."""

    def test_track_write_operation(self):
        capture = UnmanagedCapture()
        capture.track_write(
            "s3://bucket/data.parquet",
            "catalog.schema.source_table",
            {"format": "parquet"}
        )
        ops = capture.get_operations()
        assert len(ops) == 1
        assert ops[0]["type"] == "write"
        assert ops[0]["path"] == "s3://bucket/data.parquet"

    def test_track_read_operation(self):
        capture = UnmanagedCapture()
        capture.track_read(
            "s3://bucket/input.csv",
            "catalog.schema.target_table",
            {"format": "csv"}
        )
        ops = capture.get_operations()
        assert len(ops) == 1
        assert ops[0]["type"] == "read"

    def test_filter_by_path(self):
        capture = UnmanagedCapture()
        capture.track_write("s3://bucket/file1.parquet", "table1", {})
        capture.track_write("s3://bucket/file2.parquet", "table2", {})
        ops = capture.get_operations("s3://bucket/file1.parquet")
        assert len(ops) == 1
        assert ops[0]["path"] == "s3://bucket/file1.parquet"


class TestUDFMapper:
    """Test cases for UDFMapper."""

    def test_register_udf(self):
        mapper = UDFMapper()
        udf_code = "def transform(row): return row['price'] * 1.1"
        mapper.register_udf("price_adjuster", udf_code, {})
        mappings = mapper.get_column_mappings("price_adjuster")
        assert "price" in mappings

    def test_parse_multiple_columns(self):
        mapper = UDFMapper()
        code = "def calc(df): return df['col1'] + df['col2']"
        mapper.register_udf("calculator", code, {})
        mappings = mapper.get_column_mappings("calculator")
        assert "col1" in mappings
        assert "col2" in mappings

    def test_unknown_udf(self):
        mapper = UDFMapper()
        mappings = mapper.get_column_mappings("nonexistent")
        assert mappings == []


class TestLineageTracker:
    """Test cases for LineageTracker."""

    def test_record_lineage(self):
        tracker = LineageTracker()
        tracker.record_lineage(
            "source_table", "target_table", ["col1", "col2"], "transform"
        )
        lineage = tracker.get_lineage("source_table")
        assert len(lineage) == 1
        assert lineage[0]["operation"] == "transform"

    def test_get_lineage_for_target(self):
        tracker = LineageTracker()
        tracker.record_lineage("src", "tgt", ["col1"], "copy")
        lineage = tracker.get_lineage("tgt")
        assert len(lineage) == 1
