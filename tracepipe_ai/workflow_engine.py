import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum

class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    BLOCKED = "blocked"

class WorkflowEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.policy_enforcer = PolicyEnforcer(config.get("policies", {}))
        self.notification_service = NotificationService(config.get("notifications", {}))
        self.audit_trail = AuditTrail()

    def process_change(self, change_data: Dict[str, Any], impact_analysis: Dict[str, Any]) -> Dict[str, Any]:
        policy_result = self.policy_enforcer.evaluate(impact_analysis)
        
        if not policy_result["compliant"]:
            self.audit_trail.log("BLOCKED", change_data, policy_result)
            self.notification_service.send_alert("Policy Violation", policy_result)
            return {"status": ApprovalStatus.BLOCKED.value, "reason": policy_result["violations"]}
        
        approvers = self._determine_approvers(impact_analysis)
        self.audit_trail.log("APPROVAL_REQUIRED", change_data, {"approvers": approvers})
        self.notification_service.notify_approvers(approvers, change_data)
        
        return {"status": ApprovalStatus.PENDING.value, "approvers": approvers, "policy_check": policy_result}

    def _determine_approvers(self, impact_analysis: Dict[str, Any]) -> List[str]:
        severity = impact_analysis.get("severity", "low")
        approval_config = self.config.get("approvals", {})
        return approval_config.get(severity, ["default_approver"])

class PolicyEnforcer:
    def __init__(self, policies: Dict[str, Any]):
        self.policies = policies

    def evaluate(self, impact_analysis: Dict[str, Any]) -> Dict[str, Any]:
        violations = []
        
        if "max_rows_affected" in self.policies:
            rows_affected = impact_analysis.get("rows_affected", 0)
            if rows_affected > self.policies["max_rows_affected"]:
                violations.append(f"Rows affected ({rows_affected}) exceeds limit")
        
        if "blocked_operations" in self.policies:
            operation = impact_analysis.get("operation", "")
            if operation in self.policies["blocked_operations"]:
                violations.append(f"Operation '{operation}' is blocked")
        
        return {"compliant": len(violations) == 0, "violations": violations}

class NotificationService:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sent_notifications = []

    def send_alert(self, title: str, data: Dict[str, Any]):
        notification = {"type": "alert", "title": title, "data": data, "timestamp": datetime.now().isoformat()}
        self.sent_notifications.append(notification)

    def notify_approvers(self, approvers: List[str], change_data: Dict[str, Any]):
        notification = {"type": "approval_request", "approvers": approvers, "change": change_data, "timestamp": datetime.now().isoformat()}
        self.sent_notifications.append(notification)

class AuditTrail:
    def __init__(self):
        self.logs = []

    def log(self, action: str, change_data: Dict[str, Any], metadata: Dict[str, Any]):
        entry = {"action": action, "change": change_data, "metadata": metadata, "timestamp": datetime.now().isoformat()}
        self.logs.append(entry)

    def get_logs(self) -> List[Dict[str, Any]]:
        return self.logs
