"""Quality monitoring and alerting."""

from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict


class QualityMonitor:
    """Monitor and track quality metrics per lineage node."""

    def __init__(self):
        """Initialize monitor."""
        self._metrics_store: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._alerts: List[Dict[str, Any]] = []

    def record_metric(self, node_id: str, metric: Dict[str, Any]) -> None:
        """Record a quality metric for a node."""
        metric_record = {
            **metric,
            "timestamp": datetime.now().isoformat(),
            "node_id": node_id
        }
        self._metrics_store[node_id].append(metric_record)
        
        if metric.get("status") in ["warning", "critical"]:
            self._create_alert(node_id, metric_record)

    def _create_alert(self, node_id: str, metric: Dict[str, Any]) -> None:
        """Create an alert for problematic metric."""
        alert = {
            "alert_id": f"alert_{len(self._alerts) + 1}",
            "node_id": node_id,
            "metric_type": metric.get("metric_type"),
            "status": metric.get("status"),
            "timestamp": metric.get("timestamp"),
            "details": metric
        }
        self._alerts.append(alert)

    def get_node_metrics(self, node_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get metrics for a specific node."""
        return self._metrics_store.get(node_id, [])[-limit:]

    def get_latest_metric(self, node_id: str, metric_type: str) -> Optional[Dict[str, Any]]:
        """Get latest metric of specific type for a node."""
        metrics = self._metrics_store.get(node_id, [])
        for metric in reversed(metrics):
            if metric.get("metric_type") == metric_type:
                return metric
        return None

    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get all active alerts."""
        return self._alerts.copy()

    def get_alerts_for_node(self, node_id: str) -> List[Dict[str, Any]]:
        """Get alerts for specific node."""
        return [alert for alert in self._alerts if alert["node_id"] == node_id]

    def clear_alerts(self) -> None:
        """Clear all alerts."""
        self._alerts.clear()
