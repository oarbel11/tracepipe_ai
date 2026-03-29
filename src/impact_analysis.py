"""Advanced Impact Analysis & Proactive Change Alerts."""
import json
from typing import Dict, List, Any, Optional
from datetime import datetime


class ChangeSimulator:
    """Simulates changes and analyzes downstream impact."""

    def __init__(self, lineage_graph: Optional[Dict] = None):
        self.lineage_graph = lineage_graph or {}

    def simulate_schema_change(self, table_name: str, changes: Dict) -> Dict:
        """Simulate schema changes and return impact analysis."""
        affected = self._get_downstream_entities(table_name)
        return {
            "table": table_name,
            "changes": changes,
            "affected_tables": affected.get("tables", []),
            "affected_dashboards": affected.get("dashboards", []),
            "affected_models": affected.get("models", []),
            "risk_level": self._calculate_risk(changes, affected),
            "timestamp": datetime.now().isoformat()
        }

    def _get_downstream_entities(self, table_name: str) -> Dict:
        """Get all downstream entities from lineage graph."""
        if table_name not in self.lineage_graph:
            return {"tables": [], "dashboards": [], "models": []}
        
        downstream = self.lineage_graph[table_name]
        return {
            "tables": downstream.get("tables", []),
            "dashboards": downstream.get("dashboards", []),
            "models": downstream.get("models", [])
        }

    def _calculate_risk(self, changes: Dict, affected: Dict) -> str:
        """Calculate risk level based on changes and affected entities."""
        total_affected = sum(len(v) for v in affected.values())
        
        if "column_removed" in changes or "type_changed" in changes:
            return "high" if total_affected > 5 else "medium"
        elif "column_added" in changes:
            return "low"
        return "medium"


class AlertSystem:
    """Manages alerts for schema and data quality changes."""

    def __init__(self):
        self.alerts = []
        self.subscribers = {}

    def register_subscriber(self, entity: str, contacts: List[str]):
        """Register stakeholders for an entity."""
        self.subscribers[entity] = contacts

    def create_alert(self, impact_analysis: Dict) -> Dict:
        """Create alert from impact analysis."""
        alert = {
            "id": len(self.alerts) + 1,
            "table": impact_analysis["table"],
            "risk_level": impact_analysis["risk_level"],
            "changes": impact_analysis["changes"],
            "affected_count": len(impact_analysis["affected_tables"]),
            "timestamp": impact_analysis["timestamp"],
            "status": "pending"
        }
        self.alerts.append(alert)
        return alert

    def get_alerts(self, status: Optional[str] = None) -> List[Dict]:
        """Get alerts, optionally filtered by status."""
        if status:
            return [a for a in self.alerts if a["status"] == status]
        return self.alerts
