from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class PolicyEnforcer:
    def __init__(self, policies: Optional[List[Dict[str, Any]]] = None,
                 impact_analyzer: Optional[Any] = None):
        self.policies = policies or []
        self.impact_analyzer = impact_analyzer
        logger.info(f"PolicyEnforcer initialized with {len(self.policies)} policies")

    def enforce_policies(self, changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Enforce policies on changes and return violations."""
        violations = []
        
        if self.impact_analyzer:
            impact = self.impact_analyzer.analyze_changes(changes)
            risk_level = impact.get("risk_level", "low")
            
            if risk_level == "high":
                violations.append({
                    "policy": "risk_threshold",
                    "severity": "high",
                    "message": "High risk changes require senior approval"
                })

        for policy in self.policies:
            policy_violations = self._check_policy(policy, changes)
            violations.extend(policy_violations)

        return {
            "violations": violations,
            "passed": len(violations) == 0
        }

    def _check_policy(self, policy: Dict[str, Any],
                     changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check a single policy against changes."""
        violations = []
        policy_type = policy.get("type")
        
        if policy_type == "pii_tagging":
            for change in changes:
                if "pii" in change.get("file_path", "").lower():
                    if not change.get("metadata", {}).get("pii_tagged"):
                        violations.append({
                            "policy": "pii_tagging",
                            "severity": policy.get("severity", "medium"),
                            "message": "PII data must be properly tagged"
                        })
        
        elif policy_type == "data_quality":
            for change in changes:
                if "quality" in policy.get("rules", []):
                    violations.append({
                        "policy": "data_quality",
                        "severity": policy.get("severity", "low"),
                        "message": "Data quality rules must be satisfied"
                    })
        
        return violations
