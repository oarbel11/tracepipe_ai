"""Tests for lineage capture functionality."""

from scripts.lineage.unmanaged_capture import UnmanagedLineageCapture
from scripts.lineage.udf_mapper import UDFColumnMapper
from scripts.lineage.lineage_tracker import LineageTracker


def test_unmanaged_capture_write_operation():
    """Test capturing unmanaged write operations."""
    capture = UnmanagedLineageCapture()
    
    code = "df.write.save('/tmp/data.parquet')"
    context = {'source_tables': ['table1'], 'columns': ['col1', 'col2']}
    
    result = capture.capture_write_operation(code, context)
    assert result is not None
    assert result['operation_type'] == 'write'
    assert len(capture.captured_operations) == 1


def test_unmanaged_capture_get_lineage():
    """Test retrieving lineage for a path."""
    capture = UnmanagedLineageCapture()
    code = "df.write.parquet('/data/output.parquet')"
    context = {'source_tables': ['source'], 'columns': ['id']}
    
    capture.capture_write_operation(code, context)
    lineage = capture.get_lineage('/data/output.parquet')
    
    assert len(lineage) == 1


def test_udf_mapper_analyze_udf():
    """Test UDF analysis."""
    mapper = UDFColumnMapper()
    
    def sample_udf(x, y):
        return x + y
    
    result = mapper.analyze_udf(sample_udf, 'add_udf')
    assert result['udf_name'] == 'add_udf'
    assert 'x' in result['inputs']
    assert 'y' in result['inputs']


def test_udf_mapper_manual_mapping():
    """Test manual UDF mapping."""
    mapper = UDFColumnMapper()
    mapper.register_manual_mapping('custom_udf', ['in1'], ['out1'])
    
    assert 'custom_udf' in mapper.udf_mappings
    assert mapper.udf_mappings['custom_udf']['manual'] is True


def test_lineage_tracker_track_operation():
    """Test full lineage tracking."""
    tracker = LineageTracker()
    
    code = "df.write.save('/output/data.csv')"
    context = {'source_tables': ['input_table'], 'columns': ['a', 'b']}
    
    result = tracker.track_operation(code, context)
    assert result is not None
    
    lineage = tracker.get_full_lineage('/output/data.csv')
    assert lineage['source_count'] == 1


def test_lineage_tracker_export():
    """Test exporting lineage."""
    tracker = LineageTracker()
    code = "df.write.json('/data/out.json')"
    context = {'source_tables': ['src'], 'columns': ['col']}
    
    tracker.track_operation(code, context)
    export = tracker.export_lineage()
    
    assert 'lineage_graph' in export
    assert export['total_operations'] == 1
