from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime
import json

@dataclass
class Alert:
    alert_id: str
    timestamp: datetime
    severity: str
    category: str
    title: str
    message: str
    affected_objects: List[str]
    stakeholders: List[str]
    metadata: Dict[str, Any]

class AlertManager:
    def __init__(self):
        self.alert_rules = []
        self.alert_history = []
        self.notification_handlers = []

    def register_rule(self, rule_name: str, condition: Callable, severity: str, stakeholders: List[str]):
        self.alert_rules.append({
            'name': rule_name,
            'condition': condition,
            'severity': severity,
            'stakeholders': stakeholders
        })

    def register_handler(self, handler: Callable):
        self.notification_handlers.append(handler)

    def evaluate_changes(self, changes: List[Any]) -> List[Alert]:
        alerts = []
        
        for change in changes:
            for rule in self.alert_rules:
                if rule['condition'](change):
                    alert = self._create_alert(change, rule)
                    alerts.append(alert)
                    self.alert_history.append(alert)
        
        return alerts

    def _create_alert(self, change: Any, rule: Dict) -> Alert:
        alert_id = f"{rule['name']}_{datetime.now().timestamp()}"
        
        return Alert(
            alert_id=alert_id,
            timestamp=datetime.now(),
            severity=rule['severity'],
            category='schema_change',
            title=f"{rule['name']} detected",
            message=self._format_message(change, rule),
            affected_objects=self._extract_affected_objects(change),
            stakeholders=rule['stakeholders'],
            metadata={'change': str(change), 'rule': rule['name']}
        )

    def _format_message(self, change: Any, rule: Dict) -> str:
        return f"Change detected: {getattr(change, 'change_type', 'unknown')} on {getattr(change, 'affected_object', 'unknown')}"

    def _extract_affected_objects(self, change: Any) -> List[str]:
        obj = getattr(change, 'affected_object', None)
        return [obj] if obj else []

    def notify(self, alerts: List[Alert]):
        for alert in alerts:
            for handler in self.notification_handlers:
                try:
                    handler(alert)
                except Exception as e:
                    print(f"Handler failed: {e}")

    def get_alert_summary(self) -> Dict[str, Any]:
        return {
            'total_alerts': len(self.alert_history),
            'by_severity': self._count_by_severity(),
            'recent': [asdict(a) for a in self.alert_history[-10:]]
        }

    def _count_by_severity(self) -> Dict[str, int]:
        counts = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        for alert in self.alert_history:
            counts[alert.severity] = counts.get(alert.severity, 0) + 1
        return counts
