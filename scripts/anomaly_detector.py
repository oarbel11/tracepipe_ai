from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import time


@dataclass
class SchemaSnapshot:
    asset_name: str
    timestamp: float
    columns: Dict[str, str]
    row_count: Optional[int] = None
    metadata: Dict[str, Any] = None


class AnomalyDetector:
    def __init__(self, impact_analyzer=None):
        self.impact_analyzer = impact_analyzer
        self.schema_history: Dict[str, List[SchemaSnapshot]] = {}
        self.quality_thresholds = {
            "null_rate": 0.1,
            "row_count_change": 0.5,
            "column_cardinality_change": 0.3
        }

    def capture_schema(self, asset_name: str, columns: Dict[str, str],
                       row_count: Optional[int] = None) -> SchemaSnapshot:
        snapshot = SchemaSnapshot(
            asset_name=asset_name,
            timestamp=time.time(),
            columns=columns,
            row_count=row_count
        )
        if asset_name not in self.schema_history:
            self.schema_history[asset_name] = []
        self.schema_history[asset_name].append(snapshot)
        return snapshot

    def detect_schema_drift(self, asset_name: str) -> Optional[Dict]:
        if asset_name not in self.schema_history:
            return None
        history = self.schema_history[asset_name]
        if len(history) < 2:
            return None
        
        current = history[-1]
        previous = history[-2]
        
        added_cols = set(current.columns.keys()) - set(previous.columns.keys())
        removed_cols = set(previous.columns.keys()) - set(current.columns.keys())
        type_changes = {col: (previous.columns[col], current.columns[col])
                       for col in current.columns
                       if col in previous.columns and
                       current.columns[col] != previous.columns[col]}
        
        if added_cols or removed_cols or type_changes:
            drift = {
                "asset": asset_name,
                "added_columns": list(added_cols),
                "removed_columns": list(removed_cols),
                "type_changes": type_changes,
                "timestamp": current.timestamp
            }
            if self.impact_analyzer:
                msg = f"Schema drift: +{len(added_cols)} -{len(removed_cols)} cols"
                self.impact_analyzer.create_alert(
                    asset_name, "schema_drift", msg, "high"
                )
            return drift
        return None

    def detect_data_quality_anomaly(self, asset_name: str,
                                   metrics: Dict[str, float]) -> Optional[Dict]:
        anomalies = []
        if "null_rate" in metrics:
            if metrics["null_rate"] > self.quality_thresholds["null_rate"]:
                anomalies.append(f"High null rate: {metrics['null_rate']:.2%}")
        
        if "row_count" in metrics and asset_name in self.schema_history:
            history = self.schema_history[asset_name]
            if history and history[-1].row_count:
                prev_count = history[-1].row_count
                curr_count = metrics["row_count"]
                change = abs(curr_count - prev_count) / max(prev_count, 1)
                if change > self.quality_thresholds["row_count_change"]:
                    anomalies.append(f"Row count change: {change:.2%}")
        
        if anomalies and self.impact_analyzer:
            msg = "; ".join(anomalies)
            self.impact_analyzer.create_alert(
                asset_name, "data_quality", msg, "medium"
            )
            return {"asset": asset_name, "anomalies": anomalies}
        return None
