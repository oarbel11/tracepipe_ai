from typing import Dict, List, Optional
from datetime import datetime
from tracepipe_ai.data_quality_monitor import QualityIssue, DataQualityMonitor
from tracepipe_ai.lineage_quality_integrator import LineageQualityIntegrator


class QualityAlertManager:
    def __init__(self, integrator: LineageQualityIntegrator):
        self.integrator = integrator
        self.alerts: List[Dict] = []

    def generate_alert(self, asset_id: str, issue: QualityIssue) -> Dict:
        blast_radius = self.integrator.get_blast_radius(asset_id)
        alert = {
            "alert_id": f"alert_{asset_id}_{datetime.now().timestamp()}",
            "asset_id": asset_id,
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "message": issue.message,
            "detected_at": issue.detected_at.isoformat(),
            "blast_radius": blast_radius,
            "status": "active"
        }
        self.alerts.append(alert)
        return alert

    def get_active_alerts(self) -> List[Dict]:
        return [a for a in self.alerts if a["status"] == "active"]

    def resolve_alert(self, alert_id: str):
        for alert in self.alerts:
            if alert["alert_id"] == alert_id:
                alert["status"] = "resolved"
                alert["resolved_at"] = datetime.now().isoformat()
                break

    def get_alerts_for_asset(self, asset_id: str) -> List[Dict]:
        return [a for a in self.alerts if a["asset_id"] == asset_id]
