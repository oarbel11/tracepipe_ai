import pytest
from datetime import datetime, timedelta
from tracepipe_ai.data_quality_monitor import DataQualityMonitor
from tracepipe_ai.lineage_quality_integrator import LineageQualityIntegrator
from tracepipe_ai.quality_alerts import QualityAlertManager


def test_freshness_check():
    monitor = DataQualityMonitor()
    fresh_time = datetime.now() - timedelta(hours=1)
    stale_time = datetime.now() - timedelta(hours=30)

    fresh_metric = monitor.check_freshness("table_a", fresh_time, 24)
    assert fresh_metric.status == "healthy"

    stale_metric = monitor.check_freshness("table_b", stale_time, 24)
    assert stale_metric.status == "stale"


def test_volume_check():
    monitor = DataQualityMonitor()
    healthy = monitor.check_volume("table_a", 1000, 100)
    assert healthy.status == "healthy"

    anomaly = monitor.check_volume("table_b", 50, 100)
    assert anomaly.status == "anomaly"


def test_schema_check():
    monitor = DataQualityMonitor()
    expected = ["id", "name", "email"]
    current_match = ["id", "name", "email"]
    current_drift = ["id", "name", "phone"]

    match = monitor.check_schema("table_a", current_match, expected)
    assert match.status == "healthy"

    drift = monitor.check_schema("table_b", current_drift, expected)
    assert drift.status == "drift"


def test_lineage_integration():
    monitor = DataQualityMonitor()
    integrator = LineageQualityIntegrator(monitor)

    integrator.add_lineage_edge("table_a", "table_b")
    integrator.add_lineage_edge("table_b", "table_c")
    integrator.add_lineage_edge("table_b", "table_d")

    downstream = integrator.get_downstream_assets("table_a")
    assert "table_b" in downstream
    assert "table_c" in downstream
    assert "table_d" in downstream


def test_quality_propagation():
    monitor = DataQualityMonitor()
    integrator = LineageQualityIntegrator(monitor)

    integrator.add_lineage_edge("source", "transform")
    integrator.add_lineage_edge("transform", "output")

    stale_metric = monitor.check_freshness(
        "source", datetime.now() - timedelta(hours=30), 24
    )
    monitor.record_metric(stale_metric)

    affected = integrator.propagate_quality_issues("source")
    assert "transform" in affected
    assert "output" in affected


def test_alert_generation():
    monitor = DataQualityMonitor()
    integrator = LineageQualityIntegrator(monitor)
    alert_mgr = QualityAlertManager(integrator)

    integrator.add_lineage_edge("bad_table", "downstream_table")
    stale = monitor.check_freshness(
        "bad_table", datetime.now() - timedelta(hours=30), 24
    )
    monitor.record_metric(stale)

    affected = integrator.propagate_quality_issues("bad_table")
    assert len(affected) > 0

    issue = monitor.issues["bad_table"][0]
    alert = alert_mgr.generate_alert("bad_table", issue)
    assert alert["severity"] == "high"
    assert alert["status"] == "active"
