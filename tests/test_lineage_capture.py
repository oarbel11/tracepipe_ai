"""Tests for lineage capture functionality."""

import pytest
from tracepipe_ai.lineage_capture import (
    UnmanagedCapture,
    UDFMapper,
    LineageTracker
)


def test_unmanaged_capture_write_operation():
    capture = UnmanagedCapture()
    result = capture.capture_write_operation(
        path='s3://bucket/path/data.parquet',
        source_tables=['table1', 'table2'],
        columns=['col1', 'col2']
    )
    assert result is not None
    assert result['path'] == 's3://bucket/path/data.parquet'
    assert len(result['source_tables']) == 2


def test_unmanaged_capture_get_lineage():
    capture = UnmanagedCapture()
    capture.capture_write_operation(
        path='s3://bucket/data.parquet',
        source_tables=['table1']
    )
    lineage = capture.get_lineage('s3://bucket/data.parquet')
    assert len(lineage) == 1


def test_udf_mapper_analyze_udf():
    mapper = UDFMapper()
    def sample_udf(x):
        return x * 2
    mapper.register_udf('sample', sample_udf)
    result = mapper.analyze_udf('sample')
    assert 'x' in result['inputs']


def test_udf_mapper_register_udf():
    mapper = UDFMapper()
    def test_func(a, b):
        return a + b
    result = mapper.register_udf('test', test_func)
    assert result['name'] == 'test'
    assert 'test' in mapper.udf_registry


def test_lineage_tracker_track_operation():
    tracker = LineageTracker()
    result = tracker.track_operation(
        'unmanaged_write',
        {'path': 's3://bucket/test.parquet', 'source_tables': ['table1']}
    )
    assert result is not None


def test_lineage_tracker_export():
    tracker = LineageTracker()
    tracker.track_operation(
        'unmanaged_write',
        {'path': 's3://bucket/test.parquet', 'source_tables': ['table1']}
    )
    export = tracker.export_lineage()
    assert export['total_operations'] == 1
