from typing import List, Dict
from tracepipe.governance.policy_enforcement import (
    PolicyViolation,
    EnforcementEngine,
)
from tracepipe.governance.governance_policy import PolicySeverity


class ComplianceDashboard:
    def __init__(self, enforcement_engine: EnforcementEngine):
        self.engine = enforcement_engine

    def get_summary(self) -> Dict:
        violations_by_severity = {
            "low": 0,
            "medium": 0,
            "high": 0,
            "critical": 0,
        }
        for violation in self.engine.violations:
            violations_by_severity[violation.severity.value] += 1

        return {
            "total_violations": len(self.engine.violations),
            "violations_by_severity": violations_by_severity,
            "high_risk_assets": len(self.engine.get_high_risk_assets()),
            "total_alerts": len(self.engine.alerts),
            "remediation_actions": len(self.engine.actions),
        }

    def get_violations_by_asset(self) -> Dict[str, List[PolicyViolation]]:
        violations_map = {}
        for violation in self.engine.violations:
            asset_id = violation.asset_id
            if asset_id not in violations_map:
                violations_map[asset_id] = []
            violations_map[asset_id].append(violation)
        return violations_map

    def get_critical_violations(self) -> List[PolicyViolation]:
        return [
            v
            for v in self.engine.violations
            if v.severity == PolicySeverity.CRITICAL
        ]
