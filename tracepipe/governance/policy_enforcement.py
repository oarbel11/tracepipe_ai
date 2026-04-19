from typing import List, Dict, Optional
from datetime import datetime
from tracepipe.governance.governance_policy import (
    GovernancePolicy,
    PolicyViolation,
    RemediationAction,
    PolicySeverity,
)


class PolicyViolationDetector:
    def __init__(self, policies: List[GovernancePolicy]):
        self.policies = policies

    def check_asset(self, asset_id: str, asset_name: str,
                    asset_tags: List[str]) -> List[PolicyViolation]:
        violations = []
        for policy in self.policies:
            if policy.matches_asset(asset_tags):
                violation = PolicyViolation(
                    violation_id=f"V-{asset_id}-{policy.policy_id}",
                    policy=policy,
                    asset_id=asset_id,
                    asset_name=asset_name,
                    description=f"Asset violates {policy.name}",
                    severity=policy.severity,
                    detected_at=datetime.now().isoformat(),
                    auto_remediable=policy.auto_remediate,
                )
                violations.append(violation)
        return violations


class EnforcementEngine:
    def __init__(self):
        self.violations: List[PolicyViolation] = []
        self.actions: List[RemediationAction] = []
        self.alerts: List[Dict] = []

    def process_violations(self, violations: List[PolicyViolation]):
        self.violations.extend(violations)
        for violation in violations:
            self._generate_alert(violation)
            if violation.auto_remediable:
                self._trigger_remediation(violation)

    def _generate_alert(self, violation: PolicyViolation):
        alert = {
            "violation_id": violation.violation_id,
            "severity": violation.severity.value,
            "message": violation.description,
            "timestamp": datetime.now().isoformat(),
        }
        self.alerts.append(alert)

    def _trigger_remediation(self, violation: PolicyViolation):
        action = RemediationAction(
            action_id=f"A-{violation.violation_id}",
            violation_id=violation.violation_id,
            action_type="auto_remediate",
            description=f"Auto-remediate {violation.description}",
            status="triggered",
        )
        self.actions.append(action)

    def get_high_risk_assets(self) -> List[str]:
        high_risk = set()
        for v in self.violations:
            if v.severity in [PolicySeverity.HIGH, PolicySeverity.CRITICAL]:
                high_risk.add(v.asset_id)
        return list(high_risk)
