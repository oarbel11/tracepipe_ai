from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import yaml
import json

@dataclass
class WorkflowStep:
    step_id: str
    approvers: List[str]
    condition: Dict[str, Any]
    required: bool = True
    status: str = "pending"
    
class PolicyEnforcer:
    def __init__(self, policies: List[Dict]):
        self.policies = policies
    
    def enforce(self, change_data: Dict) -> Dict:
        violations = []
        for policy in self.policies:
            if self._check_violation(policy, change_data):
                violations.append({
                    "policy_id": policy["id"],
                    "severity": policy.get("severity", "medium"),
                    "message": policy["message"]
                })
        return {"violations": violations, "blocked": len(violations) > 0}
    
    def _check_violation(self, policy: Dict, data: Dict) -> bool:
        rule_type = policy.get("rule_type")
        if rule_type == "pii_check" and data.get("pii_detected"):
            return not data.get("pii_approval")
        if rule_type == "downstream_limit":
            limit = policy.get("max_downstream", 10)
            return data.get("downstream_count", 0) > limit
        return False

class NotificationService:
    def __init__(self, config: Dict):
        self.config = config
        self.sent_notifications = []
    
    def send(self, recipients: List[str], message: str, channel: str = "slack"):
        notification = {
            "timestamp": datetime.now().isoformat(),
            "recipients": recipients,
            "message": message,
            "channel": channel,
            "status": "sent"
        }
        self.sent_notifications.append(notification)
        return notification

class AuditTrail:
    def __init__(self):
        self.entries = []
    
    def log(self, change_id: str, action: str, actor: str, details: Dict):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "change_id": change_id,
            "action": action,
            "actor": actor,
            "details": details
        }
        self.entries.append(entry)
        return entry
    
    def get_trail(self, change_id: str) -> List[Dict]:
        return [e for e in self.entries if e["change_id"] == change_id]

class WorkflowEngine:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.enforcer = PolicyEnforcer(self.config.get("policies", []))
        self.notifications = NotificationService(self.config.get("notifications", {}))
        self.audit = AuditTrail()
    
    def execute_workflow(self, change_id: str, impact_analysis: Dict, author: str) -> Dict:
        self.audit.log(change_id, "workflow_started", author, impact_analysis)
        enforcement = self.enforcer.enforce(impact_analysis)
        if enforcement["blocked"]:
            self.audit.log(change_id, "blocked", "system", enforcement)
            return {"approved": False, "reason": enforcement["violations"]}
        steps = self._route_approvals(impact_analysis)
        for step in steps:
            self.notifications.send(step.approvers, f"Review required: {change_id}", "slack")
            self.audit.log(change_id, "approval_requested", "system", {"approvers": step.approvers})
        return {"approved": True, "steps": [s.step_id for s in steps]}
    
    def _route_approvals(self, impact: Dict) -> List[WorkflowStep]:
        steps = []
        if impact.get("pii_detected"):
            steps.append(WorkflowStep("pii_approval", ["privacy-team@company.com"], {"pii": True}))
        if impact.get("downstream_count", 0) > 3:
            steps.append(WorkflowStep("data_owner_approval", ["data-owner@company.com"], {"high_impact": True}))
        return steps
    
    def get_audit_trail(self, change_id: str) -> List[Dict]:
        return self.audit.get_trail(change_id)
