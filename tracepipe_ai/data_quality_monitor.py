from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class QualityMetric:
    asset_id: str
    metric_type: str
    status: str
    value: float
    threshold: float
    timestamp: datetime
    message: str = ""


@dataclass
class QualityIssue:
    asset_id: str
    issue_type: str
    severity: str
    message: str
    detected_at: datetime
    affected_downstream: List[str] = field(default_factory=list)


class DataQualityMonitor:
    def __init__(self):
        self.metrics: Dict[str, List[QualityMetric]] = {}
        self.issues: Dict[str, List[QualityIssue]] = {}
        self.asset_metadata: Dict[str, Dict] = {}

    def check_freshness(self, asset_id: str, last_update: datetime,
                       max_age_hours: float = 24) -> QualityMetric:
        age = (datetime.now() - last_update).total_seconds() / 3600
        status = "healthy" if age <= max_age_hours else "stale"
        return QualityMetric(
            asset_id=asset_id,
            metric_type="freshness",
            status=status,
            value=age,
            threshold=max_age_hours,
            timestamp=datetime.now(),
            message=f"Data is {age:.1f} hours old"
        )

    def check_volume(self, asset_id: str, row_count: int,
                    expected_min: int = 0) -> QualityMetric:
        status = "healthy" if row_count >= expected_min else "anomaly"
        return QualityMetric(
            asset_id=asset_id,
            metric_type="volume",
            status=status,
            value=float(row_count),
            threshold=float(expected_min),
            timestamp=datetime.now(),
            message=f"Row count: {row_count}"
        )

    def check_schema(self, asset_id: str, current_schema: List[str],
                    expected_schema: List[str]) -> QualityMetric:
        missing = set(expected_schema) - set(current_schema)
        extra = set(current_schema) - set(expected_schema)
        status = "healthy" if not missing and not extra else "drift"
        msg = ""
        if missing:
            msg += f"Missing columns: {missing}. "
        if extra:
            msg += f"Extra columns: {extra}"
        return QualityMetric(
            asset_id=asset_id,
            metric_type="schema",
            status=status,
            value=float(len(missing) + len(extra)),
            threshold=0.0,
            timestamp=datetime.now(),
            message=msg or "Schema matches"
        )

    def record_metric(self, metric: QualityMetric):
        if metric.asset_id not in self.metrics:
            self.metrics[metric.asset_id] = []
        self.metrics[metric.asset_id].append(metric)

    def get_asset_status(self, asset_id: str) -> str:
        if asset_id not in self.metrics or not self.metrics[asset_id]:
            return "unknown"
        recent = self.metrics[asset_id]
        statuses = [m.status for m in recent]
        if "stale" in statuses or "anomaly" in statuses or "drift" in statuses:
            return "unhealthy"
        return "healthy"
