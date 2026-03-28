"""Core data quality metrics calculation."""

from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


class MetricType(Enum):
    """Types of quality metrics."""
    FRESHNESS = "freshness"
    COMPLETENESS = "completeness"
    VOLUME = "volume"


class QualityMetrics:
    """Calculate data quality metrics."""

    @staticmethod
    def calculate_freshness(last_updated: datetime) -> Dict[str, Any]:
        """Calculate freshness metric."""
        age = datetime.now() - last_updated
        hours_old = age.total_seconds() / 3600
        
        status = "healthy" if hours_old < 24 else "warning" if hours_old < 48 else "critical"
        
        return {
            "metric_type": MetricType.FRESHNESS.value,
            "last_updated": last_updated.isoformat(),
            "hours_old": round(hours_old, 2),
            "status": status
        }

    @staticmethod
    def calculate_completeness(total_rows: int, null_count: int) -> Dict[str, Any]:
        """Calculate completeness metric."""
        completeness_pct = 100.0 if total_rows == 0 else ((total_rows - null_count) / total_rows) * 100
        
        status = "healthy" if completeness_pct >= 95 else "warning" if completeness_pct >= 85 else "critical"
        
        return {
            "metric_type": MetricType.COMPLETENESS.value,
            "total_rows": total_rows,
            "null_count": null_count,
            "completeness_pct": round(completeness_pct, 2),
            "status": status
        }

    @staticmethod
    def calculate_volume_anomaly(current_count: int, historical_avg: float, std_dev: float) -> Dict[str, Any]:
        """Calculate volume anomaly metric."""
        if std_dev == 0:
            z_score = 0.0
        else:
            z_score = (current_count - historical_avg) / std_dev
        
        status = "healthy" if abs(z_score) < 2 else "warning" if abs(z_score) < 3 else "critical"
        
        return {
            "metric_type": MetricType.VOLUME.value,
            "current_count": current_count,
            "historical_avg": round(historical_avg, 2),
            "z_score": round(z_score, 2),
            "status": status
        }
