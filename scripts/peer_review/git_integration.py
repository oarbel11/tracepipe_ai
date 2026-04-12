from typing import Dict, List, Any, Optional
import logging
from .impact_analyzer import ImpactAnalyzer
from .policy_enforcer import PolicyEnforcer

logger = logging.getLogger(__name__)


class GitCICDIntegration:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            config = {}
        
        self.config = config
        self.impact_analyzer = ImpactAnalyzer()
        self.policy_enforcer = PolicyEnforcer(
            policies=config.get("policies", []),
            impact_analyzer=self.impact_analyzer
        )
        logger.info("GitCICDIntegration initialized")

    def process_commit(self, commit_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a commit and return analysis results."""
        changes = self._extract_changes(commit_data)
        
        impact = self.impact_analyzer.analyze_changes(changes)
        policy_result = self.policy_enforcer.enforce_policies(changes)
        
        return {
            "commit_sha": commit_data.get("sha", ""),
            "impact": impact,
            "policy_check": policy_result,
            "approved": policy_result["passed"] and impact["risk_level"] != "high"
        }

    def handle_webhook(self, event_type: str,
                      payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle webhook events from Git providers."""
        if event_type not in ["push", "pull_request"]:
            return {"status": "ignored", "message": f"Event type {event_type} not supported"}
        
        if event_type == "push":
            commits = payload.get("commits", [])
            results = [self.process_commit(commit) for commit in commits]
            return {"status": "processed", "results": results}
        
        return {"status": "success"}

    def _extract_changes(self, commit_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract changes from commit data."""
        changes = []
        for file_path in commit_data.get("modified", []):
            changes.append({"file_path": file_path, "change_type": "modified"})
        for file_path in commit_data.get("added", []):
            changes.append({"file_path": file_path, "change_type": "added"})
        for file_path in commit_data.get("removed", []):
            changes.append({"file_path": file_path, "change_type": "deleted"})
        return changes
