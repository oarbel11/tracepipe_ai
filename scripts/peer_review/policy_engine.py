from typing import Dict, List, Optional
from dataclasses import dataclass
from scripts.peer_review.governance_policy import GovernancePolicy
import json

@dataclass
class PolicyViolation:
    policy_id: str
    asset_id: str
    severity: str
    description: str
    metadata: Dict

    def to_dict(self) -> Dict:
        return {
            'policy_id': self.policy_id,
            'asset_id': self.asset_id,
            'severity': self.severity,
            'description': self.description,
            'metadata': self.metadata
        }

class PolicyEngine:
    def __init__(self):
        self.policies: Dict[str, GovernancePolicy] = {}

    def add_policy(self, policy: GovernancePolicy):
        self.policies[policy.policy_id] = policy

    def remove_policy(self, policy_id: str):
        if policy_id in self.policies:
            del self.policies[policy_id]

    def evaluate_asset(self, asset_id: str, tags: List[str], metadata: Dict) -> List[PolicyViolation]:
        violations = []
        for policy in self.policies.values():
            if policy.matches_asset(tags, asset_id):
                if self._check_violation(policy, metadata):
                    violations.append(PolicyViolation(
                        policy_id=policy.policy_id,
                        asset_id=asset_id,
                        severity=policy.severity,
                        description=f"Policy '{policy.name}' violated",
                        metadata={'rules': policy.rules, 'asset_metadata': metadata}
                    ))
        return violations

    def _check_violation(self, policy: GovernancePolicy, metadata: Dict) -> bool:
        for rule_key, rule_value in policy.rules.items():
            asset_value = str(metadata.get(rule_key, ''))
            if rule_value.lower() == 'true' and asset_value.lower() != 'true':
                continue
            if rule_value != asset_value:
                return True
        return len(policy.rules) > 0

    def get_all_policies(self) -> List[GovernancePolicy]:
        return list(self.policies.values())
