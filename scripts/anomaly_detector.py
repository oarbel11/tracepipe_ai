from typing import Dict, List, Optional
from datetime import datetime

class Anomaly:
    def __init__(self, asset_id: str, anomaly_type: str, details: Dict):
        self.asset_id = asset_id
        self.anomaly_type = anomaly_type
        self.details = details
        self.timestamp = datetime.utcnow().isoformat()

class AnomalyDetector:
    def __init__(self):
        self.anomalies: List[Anomaly] = []
        self.baseline_stats: Dict[str, Dict] = {}

    def set_baseline(self, asset_id: str, stats: Dict):
        self.baseline_stats[asset_id] = stats

    def detect_data_quality_anomaly(self, asset_id: str, current_stats: Dict) -> Optional[Anomaly]:
        if asset_id not in self.baseline_stats:
            return None
        
        baseline = self.baseline_stats[asset_id]
        anomalies_found = []
        
        for metric, baseline_value in baseline.items():
            current_value = current_stats.get(metric)
            if current_value is None:
                continue
            
            if isinstance(baseline_value, (int, float)) and isinstance(current_value, (int, float)):
                if baseline_value > 0:
                    change_pct = abs(current_value - baseline_value) / baseline_value
                    if change_pct > 0.2:
                        anomalies_found.append(f"{metric}: {change_pct*100:.1f}% change")
        
        if anomalies_found:
            anomaly = Anomaly(asset_id, "data_quality", {
                "baseline": baseline,
                "current": current_stats,
                "issues": anomalies_found
            })
            self.anomalies.append(anomaly)
            return anomaly
        return None

    def get_anomalies(self) -> List[Anomaly]:
        return self.anomalies
