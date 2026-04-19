from typing import Dict, List, Optional
from .impact_analyzer import ImpactAnalyzer
from .workflow_engine import WorkflowEngine
import os

class PeerReviewOrchestrator:
    def __init__(self, workflow_config_path: Optional[str] = None):
        if workflow_config_path is None:
            workflow_config_path = os.path.join(
                os.path.dirname(__file__), 
                '../../config/workflow_config.yml'
            )
        self.workflow_engine = WorkflowEngine(workflow_config_path)
        self.impact_analyzer = None
    
    def review_change(self, change_id: str, change_details: Dict, author: str) -> Dict:
        impact = self._analyze_impact(change_details)
        workflow_result = self.workflow_engine.execute_workflow(
            change_id=change_id,
            impact_analysis=impact,
            author=author
        )
        return {
            "change_id": change_id,
            "impact_analysis": impact,
            "workflow_result": workflow_result,
            "audit_trail": self.workflow_engine.get_audit_trail(change_id)
        }
    
    def _analyze_impact(self, change_details: Dict) -> Dict:
        pii_keywords = ["email", "ssn", "phone", "address"]
        columns = change_details.get("columns", [])
        pii_detected = any(kw in str(columns).lower() for kw in pii_keywords)
        
        return {
            "pii_detected": pii_detected,
            "downstream_count": change_details.get("downstream_count", 0),
            "schema_changed": change_details.get("schema_changed", False),
            "risk_level": "high" if pii_detected else "medium"
        }
    
    def get_review_status(self, change_id: str) -> Dict:
        audit_trail = self.workflow_engine.get_audit_trail(change_id)
        if not audit_trail:
            return {"status": "not_found"}
        
        latest = audit_trail[-1]
        return {
            "status": latest["action"],
            "audit_trail": audit_trail,
            "notifications": self.workflow_engine.notifications.sent_notifications
        }
