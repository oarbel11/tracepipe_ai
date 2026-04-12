"""Git-based CI/CD integration."""
from typing import Dict, Any, Optional
from .impact_analyzer import InteractiveImpactAnalyzer
from .policy_enforcer import CICDPolicyEnforcer
from .approval_workflow import ApprovalWorkflow


class GitCICDIntegration:
    """Integrates with Git-based CI/CD workflows."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.analyzer = InteractiveImpactAnalyzer(config)
        self.enforcer = CICDPolicyEnforcer(
            policies=config.get("policies", []),
            ci_config=config.get("ci_config", {})
        )
        self.workflow = ApprovalWorkflow(
            approvers=config.get("approvers", [])
        )
    
    def process_commit(self, commit_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a commit in the CI/CD pipeline."""
        impact = self.analyzer.analyze_changes(commit_data)
        policy_result = self.enforcer.enforce_in_pipeline(commit_data)
        
        if not policy_result["compliant"]:
            return {
                "status": "failed",
                "reason": "Policy violations",
                "violations": policy_result["violations"]
            }
        
        if impact["impact"]["risk_level"] in ["high", "critical"]:
            approval = self.workflow.request_approval(
                commit_data.get("commit_id", "unknown"),
                {"impact": impact, "policy": policy_result}
            )
            return {"status": "approval_required", "approval": approval}
        
        return {"status": "success", "impact": impact, 
                "policy": policy_result}
    
    def webhook_handler(self, event: str, 
                       payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Git webhook events."""
        if event == "push":
            return self.process_commit(payload)
        return {"status": "ignored", "event": event}
