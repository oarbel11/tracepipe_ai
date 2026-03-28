from typing import List, Dict, Optional
from datetime import datetime
import duckdb
from .quality_metrics import QualityMetrics, QualityMetric, MetricType

class QualityMonitor:
    def __init__(self, conn):
        self.conn = conn
        self.metrics_engine = QualityMetrics(conn)
        self._init_storage()

    def _init_storage(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_metrics (
                id INTEGER PRIMARY KEY,
                node_id VARCHAR,
                metric_type VARCHAR,
                value DOUBLE,
                threshold DOUBLE,
                status VARCHAR,
                timestamp TIMESTAMP,
                details VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_alerts (
                id INTEGER PRIMARY KEY,
                node_id VARCHAR,
                metric_type VARCHAR,
                severity VARCHAR,
                message VARCHAR,
                created_at TIMESTAMP,
                resolved_at TIMESTAMP
            )
        """)

    def monitor_table(self, table: str, config: Dict) -> List[QualityMetric]:
        metrics = []
        
        if config.get("freshness"):
            m = self.metrics_engine.calculate_freshness(
                table, config["freshness"]["timestamp_col"], config["freshness"].get("threshold_hours", 24)
            )
            metrics.append(m)
            self._store_metric(m)
            if not m.is_healthy():
                self._create_alert(m)
        
        if config.get("completeness"):
            m = self.metrics_engine.calculate_completeness(table, config["completeness"]["columns"])
            metrics.append(m)
            self._store_metric(m)
            if not m.is_healthy():
                self._create_alert(m)
        
        if config.get("volume"):
            m = self.metrics_engine.detect_volume_anomaly(table, config["volume"].get("lookback_days", 7))
            metrics.append(m)
            self._store_metric(m)
        
        return metrics

    def _store_metric(self, metric: QualityMetric):
        self.conn.execute("""
            INSERT INTO quality_metrics (node_id, metric_type, value, threshold, status, timestamp, details)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [metric.node_id, metric.metric_type.value, metric.value, metric.threshold, 
               metric.status, metric.timestamp, str(metric.details)])

    def _create_alert(self, metric: QualityMetric):
        severity = "critical" if metric.status == "critical" else "warning"
        message = f"{metric.metric_type.value} issue: {metric.value:.2f} (threshold: {metric.threshold})"
        self.conn.execute("""
            INSERT INTO quality_alerts (node_id, metric_type, severity, message, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, [metric.node_id, metric.metric_type.value, severity, message, datetime.now()])

    def get_node_metrics(self, node_id: str) -> List[Dict]:
        result = self.conn.execute("""
            SELECT metric_type, value, status, timestamp 
            FROM quality_metrics WHERE node_id = ? ORDER BY timestamp DESC LIMIT 10
        """, [node_id]).fetchall()
        return [{"type": r[0], "value": r[1], "status": r[2], "timestamp": r[3]} for r in result]
