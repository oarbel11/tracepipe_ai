from typing import Dict, List, Optional
import json
from scripts.impact_analysis import ImpactAnalyzer
from scripts.anomaly_detector import AnomalyDetector


class ImpactDashboard:
    def __init__(self):
        self.analyzer = ImpactAnalyzer()
        self.detector = AnomalyDetector(self.analyzer)

    def get_lineage_graph(self, asset_name: str) -> Dict:
        upstream = list(self.analyzer.graph.predecessors(asset_name)) \
            if asset_name in self.analyzer.graph else []
        downstream = list(self.analyzer.graph.successors(asset_name)) \
            if asset_name in self.analyzer.graph else []
        
        asset_id = self.analyzer.name_mapping.get(asset_name, asset_name)
        asset = self.analyzer.assets.get(asset_id)
        
        return {
            "asset": asset_name,
            "upstream": upstream,
            "downstream": downstream,
            "previous_names": asset.previous_names if asset else [],
            "version_count": len(asset.versions) if asset else 0
        }

    def get_asset_history(self, asset_name: str) -> List[Dict]:
        asset_id = self.analyzer.name_mapping.get(asset_name, asset_name)
        asset = self.analyzer.assets.get(asset_id)
        if not asset:
            return []
        
        return [{
            "version": v.version,
            "timestamp": v.timestamp,
            "upstream_count": len(v.upstream),
            "downstream_count": len(v.downstream)
        } for v in asset.versions]

    def get_dashboard_summary(self) -> Dict:
        return {
            "total_assets": len(self.analyzer.assets),
            "total_lineage_edges": self.analyzer.graph.number_of_edges(),
            "active_alerts": len(self.analyzer.get_active_alerts()),
            "tracked_schemas": len(self.detector.schema_history),
            "alerts_by_severity": self._group_alerts_by_severity()
        }

    def _group_alerts_by_severity(self) -> Dict[str, int]:
        alerts = self.analyzer.get_active_alerts()
        severity_counts = {"high": 0, "medium": 0, "low": 0}
        for alert in alerts:
            severity_counts[alert["severity"]] = \
                severity_counts.get(alert["severity"], 0) + 1
        return severity_counts

    def resolve_asset_name(self, name_or_old_name: str) -> Optional[str]:
        asset_id = self.analyzer.name_mapping.get(name_or_old_name)
        if asset_id and asset_id in self.analyzer.assets:
            return self.analyzer.assets[asset_id].current_name
        return None

    def export_state(self) -> str:
        state = {
            "assets": {k: {
                "current_name": v.current_name,
                "previous_names": v.previous_names,
                "version_count": len(v.versions)
            } for k, v in self.analyzer.assets.items()},
            "alerts": self.analyzer.get_active_alerts(),
            "summary": self.get_dashboard_summary()
        }
        return json.dumps(state, indent=2)
