"""Tests for data quality module."""

import pytest
from datetime import datetime, timedelta
from tracepipe_ai.data_quality import QualityMetrics, MetricType, QualityMonitor, LineageIntegrator


class TestQualityMetrics:
    """Test quality metrics calculation."""

    def test_freshness_healthy(self):
        """Test freshness calculation - healthy."""
        last_updated = datetime.now() - timedelta(hours=12)
        result = QualityMetrics.calculate_freshness(last_updated)
        assert result["metric_type"] == MetricType.FRESHNESS.value
        assert result["status"] == "healthy"
        assert result["hours_old"] < 24

    def test_freshness_critical(self):
        """Test freshness calculation - critical."""
        last_updated = datetime.now() - timedelta(hours=72)
        result = QualityMetrics.calculate_freshness(last_updated)
        assert result["status"] == "critical"

    def test_completeness_healthy(self):
        """Test completeness calculation - healthy."""
        result = QualityMetrics.calculate_completeness(1000, 10)
        assert result["metric_type"] == MetricType.COMPLETENESS.value
        assert result["status"] == "healthy"
        assert result["completeness_pct"] >= 95

    def test_completeness_critical(self):
        """Test completeness calculation - critical."""
        result = QualityMetrics.calculate_completeness(1000, 200)
        assert result["status"] == "critical"

    def test_volume_anomaly_healthy(self):
        """Test volume anomaly - healthy."""
        result = QualityMetrics.calculate_volume_anomaly(100, 100, 10)
        assert result["metric_type"] == MetricType.VOLUME.value
        assert result["status"] == "healthy"

    def test_volume_anomaly_critical(self):
        """Test volume anomaly - critical."""
        result = QualityMetrics.calculate_volume_anomaly(200, 100, 10)
        assert result["status"] == "critical"


class TestQualityMonitor:
    """Test quality monitoring."""

    def test_record_metric(self):
        """Test recording a metric."""
        monitor = QualityMonitor()
        metric = {"metric_type": "freshness", "status": "healthy"}
        monitor.record_metric("node1", metric)
        metrics = monitor.get_node_metrics("node1")
        assert len(metrics) == 1

    def test_alert_creation(self):
        """Test alert creation for warning status."""
        monitor = QualityMonitor()
        metric = {"metric_type": "freshness", "status": "warning"}
        monitor.record_metric("node1", metric)
        alerts = monitor.get_active_alerts()
        assert len(alerts) == 1
        assert alerts[0]["status"] == "warning"


class TestLineageIntegrator:
    """Test lineage integration."""

    def test_enrich_node(self):
        """Test enriching a single node."""
        monitor = QualityMonitor()
        integrator = LineageIntegrator(monitor)
        node = {"id": "node1", "name": "table1"}
        enriched = integrator.enrich_lineage_node(node)
        assert "quality_metrics" in enriched
        assert "quality_status" in enriched
