"""Tests for operational lineage tracking."""

import pytest
import json
from tracepipe_ai.operational_lineage import (
    LineageCapture, LineageGraph, LineageVisualizer
)


def test_lineage_capture_notebook():
    """Test capturing notebook lineage."""
    capture = LineageCapture()
    record = capture.capture_notebook_lineage(
        '/notebooks/etl',
        ['source.table1'],
        ['target.table2']
    )
    assert record['type'] == 'notebook'
    assert record['asset_id'] == '/notebooks/etl'
    assert 'source.table1' in record['tables_read']
    assert 'target.table2' in record['tables_written']


def test_lineage_capture_job():
    """Test capturing job lineage."""
    capture = LineageCapture()
    record = capture.capture_job_lineage(
        'job123', 'ETL Job',
        ['source.table1'],
        ['target.table2']
    )
    assert record['type'] == 'job'
    assert record['asset_id'] == 'job123'


def test_lineage_graph_building():
    """Test building lineage graph from records."""
    capture = LineageCapture()
    capture.capture_notebook_lineage(
        '/notebooks/etl', ['source.t1'], ['target.t1']
    )
    capture.capture_job_lineage(
        'job1', 'Job', ['target.t1'], ['target.t2']
    )

    graph = LineageGraph()
    graph.build_from_records(capture.get_all_records())

    assert len(graph.nodes) == 5
    assert 'source.t1' in graph.nodes
    assert '/notebooks/etl' in graph.nodes


def test_lineage_graph_downstream():
    """Test getting downstream nodes."""
    graph = LineageGraph()
    graph.add_node('notebook1', 'notebook')
    graph.add_node('table1', 'table')
    graph.add_edge('notebook1', 'table1', 'produces')

    downstream = graph.get_downstream('notebook1')
    assert 'table1' in downstream


def test_lineage_visualizer_json():
    """Test JSON export."""
    graph = LineageGraph()
    graph.add_node('notebook1', 'notebook')
    graph.add_node('table1', 'table')
    graph.add_edge('notebook1', 'table1', 'produces')

    visualizer = LineageVisualizer(graph)
    output = visualizer.to_json()
    data = json.loads(output)

    assert len(data['nodes']) == 2
    assert len(data['edges']) == 1


def test_lineage_visualizer_text():
    """Test text visualization."""
    graph = LineageGraph()
    graph.add_node('notebook1', 'notebook')
    visualizer = LineageVisualizer(graph)
    text = visualizer.to_text()
    assert 'Operational Lineage Graph' in text
