from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, field
from scripts.peer_review.governance_policy import GovernancePolicy, PolicyViolation

class PolicyViolationDetector:
    """Detects policy violations in assets based on governance policies."""
    
    def __init__(self, policies: List[GovernancePolicy]):
        self.policies = policies
    
    def detect_violations(self, assets: List[Dict]) -> List[PolicyViolation]:
        """Detect all policy violations across assets."""
        violations = []
        for asset in assets:
            for policy in self.policies:
                violation = policy.check_compliance(asset)
                if violation:
                    violations.append(violation)
        return violations
    
    def get_high_risk_assets(self, assets: List[Dict], 
                            upstream_changes: Dict[str, List[str]]) -> List[Dict]:
        """Identify assets at high risk due to upstream changes."""
        high_risk = []
        for asset in assets:
            asset_id = asset.get('id', '')
            if asset_id in upstream_changes and upstream_changes[asset_id]:
                asset_copy = asset.copy()
                asset_copy['risk_reason'] = f"Upstream changes: {len(upstream_changes[asset_id])}"
                asset_copy['changed_dependencies'] = upstream_changes[asset_id]
                high_risk.append(asset_copy)
        return high_risk

@dataclass
class RemediationAction:
    """Represents a remediation action for a policy violation."""
    action_type: str
    description: str
    automated: bool = False
    params: Dict = field(default_factory=dict)

class EnforcementEngine:
    """Engine for enforcing governance policies through alerts and actions."""
    
    def __init__(self):
        self.alert_handlers: List[Callable] = []
        self.remediation_suggestions = {
            'pii': RemediationAction('mask_data', 'Apply data masking to PII fields', False),
            'quality': RemediationAction('request_review', 'Request owner review for quality', False),
            'sensitive': RemediationAction('open_ticket', 'Open approval workflow ticket', False)
        }
    
    def register_alert_handler(self, handler: Callable):
        """Register a custom alert handler."""
        self.alert_handlers.append(handler)
    
    def trigger_alert(self, violation: PolicyViolation) -> Dict:
        """Trigger alert for policy violation."""
        alert = {
            'type': 'policy_violation',
            'severity': violation.policy.severity,
            'asset_id': violation.asset_id,
            'policy_name': violation.policy.name,
            'violations': violation.violations,
            'timestamp': violation.timestamp
        }
        for handler in self.alert_handlers:
            handler(alert)
        return alert
    
    def suggest_remediation(self, violation: PolicyViolation) -> List[RemediationAction]:
        """Suggest remediation actions based on policy tags."""
        suggestions = []
        for tag in violation.policy.tags:
            if tag in self.remediation_suggestions:
                suggestions.append(self.remediation_suggestions[tag])
        return suggestions
    
    def enforce_violations(self, violations: List[PolicyViolation]) -> Dict:
        """Enforce all violations by triggering alerts and suggesting actions."""
        results = {'alerts': [], 'remediations': {}}
        for violation in violations:
            alert = self.trigger_alert(violation)
            results['alerts'].append(alert)
            suggestions = self.suggest_remediation(violation)
            results['remediations'][violation.asset_id] = [
                {'type': s.action_type, 'description': s.description} 
                for s in suggestions
            ]
        return results
