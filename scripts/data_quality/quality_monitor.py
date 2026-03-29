import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class QualityMetric:
    asset_name: str
    metric_type: str
    value: float
    threshold: float
    is_anomaly: bool
    timestamp: datetime
    severity: str


@dataclass
class QualityIssue:
    asset_name: str
    issue_type: str
    description: str
    severity: str
    detected_at: datetime
    affected_downstream: List[str]


class DataQualityMonitor:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._init_quality_tables()

    def _init_quality_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_metrics (
                asset_name VARCHAR,
                metric_type VARCHAR,
                value DOUBLE,
                threshold DOUBLE,
                timestamp TIMESTAMP,
                severity VARCHAR
            )
        """)

    def check_freshness(self, table_name: str, max_age_hours: int = 24) -> Optional[QualityIssue]:
        try:
            result = self.conn.execute(f"""
                SELECT MAX(timestamp) as last_update
                FROM {table_name}
            """).fetchone()
            
            if result and result[0]:
                age = datetime.now() - result[0]
                is_stale = age > timedelta(hours=max_age_hours)
                
                if is_stale:
                    return QualityIssue(
                        asset_name=table_name,
                        issue_type="freshness",
                        description=f"Data is {age.total_seconds()/3600:.1f}h old",
                        severity="high" if age.total_seconds() > max_age_hours * 2 * 3600 else "medium",
                        detected_at=datetime.now(),
                        affected_downstream=[]
                    )
        except Exception:
            pass
        return None

    def check_volume_anomaly(self, table_name: str, threshold_pct: float = 0.3) -> Optional[QualityIssue]:
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) as current_count
                FROM {table_name}
            """).fetchone()
            
            if result:
                current = result[0]
                baseline = self._get_baseline_volume(table_name)
                
                if baseline and abs(current - baseline) / baseline > threshold_pct:
                    return QualityIssue(
                        asset_name=table_name,
                        issue_type="volume",
                        description=f"Volume anomaly: {current} vs baseline {baseline}",
                        severity="medium",
                        detected_at=datetime.now(),
                        affected_downstream=[]
                    )
        except Exception:
            pass
        return None

    def _get_baseline_volume(self, table_name: str) -> Optional[int]:
        return None

    def get_all_issues(self, tables: List[str]) -> List[QualityIssue]:
        issues = []
        for table in tables:
            issue = self.check_freshness(table)
            if issue:
                issues.append(issue)
            issue = self.check_volume_anomaly(table)
            if issue:
                issues.append(issue)
        return issues
