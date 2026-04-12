from typing import Dict, List, Any
from scripts.impact_analysis import ImpactAnalyzer, Alert
from scripts.anomaly_detector import AnomalyDetector

class ImpactDashboard:
    def __init__(self):
        self.analyzer = ImpactAnalyzer()
        self.detector = AnomalyDetector()

    def get_impact_summary(self, asset_id: str) -> Dict[str, Any]:
        downstream = list(self.analyzer.get_downstream_assets(asset_id))
        versions = self.analyzer.lineage_versions.get(asset_id, [])
        
        return {
            "asset_id": asset_id,
            "downstream_count": len(downstream),
            "downstream_assets": downstream,
            "version_count": len(versions),
            "current_version": versions[-1].version if versions else 0
        }

    def get_alerts(self, severity: str = None) -> List[Dict[str, Any]]:
        alerts = self.analyzer.get_alerts()
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return [{
            "asset_id": a.asset_id,
            "message": a.message,
            "severity": a.severity,
            "affected_count": len(a.affected_assets),
            "affected_assets": a.affected_assets,
            "timestamp": a.timestamp
        } for a in alerts]

    def get_anomalies(self) -> List[Dict[str, Any]]:
        anomalies = self.detector.get_anomalies()
        return [{
            "asset_id": a.asset_id,
            "type": a.anomaly_type,
            "details": a.details,
            "timestamp": a.timestamp
        } for a in anomalies]

    def track_asset(self, asset_id: str, schema: Dict, upstream: List[str], downstream: List[str]):
        self.analyzer.track_lineage(asset_id, schema, upstream, downstream)

    def rename_asset(self, old_id: str, new_id: str):
        self.analyzer.handle_rename(old_id, new_id)

    def check_schema_change(self, asset_id: str, new_schema: Dict) -> bool:
        alert = self.analyzer.detect_schema_change(asset_id, new_schema)
        return alert is not None
