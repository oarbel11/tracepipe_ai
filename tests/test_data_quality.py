import pytest
import duckdb
from datetime import datetime, timedelta
from scripts.data_quality import QualityMetrics, QualityMonitor, LineageQualityIntegrator, MetricType
import networkx as nx

@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    conn.execute("INSERT INTO test_table VALUES (1, 'Alice', 'alice@test.com', TIMESTAMP '2024-01-15 10:00:00')")
    conn.execute("INSERT INTO test_table VALUES (2, 'Bob', NULL, TIMESTAMP '2024-01-15 11:00:00')")
    conn.execute("INSERT INTO test_table VALUES (3, 'Charlie', 'charlie@test.com', TIMESTAMP '2024-01-15 12:00:00')")
    return conn

@pytest.fixture
def quality_metrics(db_conn):
    return QualityMetrics(db_conn)

@pytest.fixture
def quality_monitor(db_conn):
    return QualityMonitor(db_conn)

def test_freshness_calculation(quality_metrics):
    metric = quality_metrics.calculate_freshness("test_table", "updated_at", threshold_hours=1)
    assert metric.metric_type == MetricType.FRESHNESS
    assert metric.node_id == "test_table"
    assert metric.status == "stale"

def test_completeness_calculation(quality_metrics):
    metric = quality_metrics.calculate_completeness("test_table", ["name", "email"])
    assert metric.metric_type == MetricType.COMPLETENESS
    assert metric.value < 100.0
    assert metric.status in ["healthy", "warning", "critical"]

def test_volume_anomaly_detection(quality_metrics):
    metric = quality_metrics.detect_volume_anomaly("test_table")
    assert metric.metric_type == MetricType.VOLUME
    assert metric.value == 3

def test_monitor_table(quality_monitor):
    config = {
        "freshness": {"timestamp_col": "updated_at", "threshold_hours": 1},
        "completeness": {"columns": ["name", "email"]}
    }
    metrics = quality_monitor.monitor_table("test_table", config)
    assert len(metrics) == 2
    assert any(m.metric_type == MetricType.FRESHNESS for m in metrics)
    assert any(m.metric_type == MetricType.COMPLETENESS for m in metrics)

def test_get_node_metrics(quality_monitor):
    config = {"freshness": {"timestamp_col": "updated_at"}}
    quality_monitor.monitor_table("test_table", config)
    metrics = quality_monitor.get_node_metrics("test_table")
    assert len(metrics) > 0
    assert "type" in metrics[0]

def test_lineage_integration(quality_monitor):
    graph = nx.DiGraph()
    graph.add_edge("table_a", "table_b")
    graph.add_edge("table_b", "table_c")
    
    integrator = LineageQualityIntegrator(quality_monitor)
    enriched = integrator.enrich_lineage_graph(graph)
    
    assert "quality_status" in enriched.nodes["table_a"]
    assert "quality_metrics" in enriched.nodes["table_a"]

def test_impact_summary(quality_monitor):
    graph = nx.DiGraph()
    graph.add_edge("source", "downstream1")
    graph.add_edge("downstream1", "downstream2")
    
    integrator = LineageQualityIntegrator(quality_monitor)
    enriched = integrator.enrich_lineage_graph(graph)
    summary = integrator.get_impact_summary(enriched, "source")
    
    assert "source" in summary
    assert "total_downstream" in summary
    assert summary["total_downstream"] == 2
