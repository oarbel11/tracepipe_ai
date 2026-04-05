from typing import Dict, List, Any, Optional
from datetime import datetime


class DataQualityMetrics:
    def __init__(self):
        self.metrics = {}
        self.alerts = []

    def add_metric(self, table: str, metric_type: str, value: float,
                   threshold: Optional[float] = None):
        if table not in self.metrics:
            self.metrics[table] = []
        metric = {
            "type": metric_type,
            "value": value,
            "threshold": threshold,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.metrics[table].append(metric)
        if threshold and value > threshold:
            self.create_alert(table, metric_type, value, threshold)

    def create_alert(self, table: str, metric_type: str, value: float,
                     threshold: float):
        alert = {
            "table": table,
            "metric_type": metric_type,
            "value": value,
            "threshold": threshold,
            "severity": "high" if value > threshold * 1.5 else "medium",
            "timestamp": datetime.utcnow().isoformat()
        }
        self.alerts.append(alert)

    def get_metrics(self, table: str) -> List[Dict[str, Any]]:
        return self.metrics.get(table, [])

    def get_alerts(self, table: Optional[str] = None) -> List[Dict[str, Any]]:
        if table:
            return [a for a in self.alerts if a["table"] == table]
        return self.alerts

    def get_quality_score(self, table: str) -> float:
        table_metrics = self.get_metrics(table)
        if not table_metrics:
            return 100.0
        violations = sum(1 for m in table_metrics
                        if m["threshold"] and m["value"] > m["threshold"])
        return max(0.0, 100.0 - (violations / len(table_metrics)) * 100)
