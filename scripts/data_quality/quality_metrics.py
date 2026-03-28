from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import duckdb

class MetricType(Enum):
    FRESHNESS = "freshness"
    COMPLETENESS = "completeness"
    VOLUME = "volume"
    SCHEMA = "schema"

@dataclass
class QualityMetric:
    node_id: str
    metric_type: MetricType
    value: float
    threshold: float
    status: str
    timestamp: datetime
    details: Dict[str, Any]

    def is_healthy(self) -> bool:
        return self.status == "healthy"

class QualityMetrics:
    def __init__(self, conn):
        self.conn = conn

    def calculate_freshness(self, table: str, timestamp_col: str, threshold_hours: int = 24) -> QualityMetric:
        query = f"SELECT MAX({timestamp_col}) as last_update FROM {table}"
        result = self.conn.execute(query).fetchone()
        last_update = result[0] if result else None
        
        if not last_update:
            return QualityMetric(table, MetricType.FRESHNESS, 0, threshold_hours, "critical", datetime.now(), {})
        
        hours_old = (datetime.now() - last_update).total_seconds() / 3600
        status = "healthy" if hours_old <= threshold_hours else "stale"
        
        return QualityMetric(table, MetricType.FRESHNESS, hours_old, threshold_hours, status, datetime.now(), {"last_update": str(last_update)})

    def calculate_completeness(self, table: str, columns: list) -> QualityMetric:
        total = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if total == 0:
            return QualityMetric(table, MetricType.COMPLETENESS, 100.0, 95.0, "healthy", datetime.now(), {})
        
        null_checks = [f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)" for col in columns]
        query = f"SELECT {', '.join(null_checks)} FROM {table}"
        nulls = self.conn.execute(query).fetchone()
        
        avg_null_pct = sum(nulls) / (total * len(columns)) * 100
        completeness = 100 - avg_null_pct
        status = "healthy" if completeness >= 95 else "warning" if completeness >= 80 else "critical"
        
        return QualityMetric(table, MetricType.COMPLETENESS, completeness, 95.0, status, datetime.now(), {"total_rows": total})

    def detect_volume_anomaly(self, table: str, lookback_days: int = 7) -> QualityMetric:
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        status = "healthy"
        details = {"current_count": count}
        
        return QualityMetric(table, MetricType.VOLUME, count, 0, status, datetime.now(), details)
