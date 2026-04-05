from typing import Dict, List, Any, Optional
from enum import Enum

class PolicyType(Enum):
    SCHEMA_CHANGE = "schema_change"
    FRESHNESS = "freshness"
    PII_DETECTION = "pii_detection"
    DATA_QUALITY = "data_quality"

class Policy:
    def __init__(self, policy_id: str, asset_id: str, policy_type: PolicyType,
                 rules: Dict[str, Any], enabled: bool = True):
        self.policy_id = policy_id
        self.asset_id = asset_id
        self.policy_type = policy_type
        self.rules = rules
        self.enabled = enabled

class PolicyEngine:
    def __init__(self):
        self.policies: Dict[str, Policy] = {}

    def create_policy(self, policy_id: str, asset_id: str,
                      policy_type: str, rules: Dict[str, Any]) -> Policy:
        policy_type_enum = PolicyType(policy_type)
        policy = Policy(policy_id, asset_id, policy_type_enum, rules)
        self.policies[policy_id] = policy
        return policy

    def get_policies_for_asset(self, asset_id: str) -> List[Policy]:
        return [p for p in self.policies.values()
                if p.asset_id == asset_id and p.enabled]

    def get_policy(self, policy_id: str) -> Optional[Policy]:
        return self.policies.get(policy_id)

    def update_policy(self, policy_id: str, rules: Dict[str, Any]) -> bool:
        if policy_id in self.policies:
            self.policies[policy_id].rules = rules
            return True
        return False

    def delete_policy(self, policy_id: str) -> bool:
        if policy_id in self.policies:
            del self.policies[policy_id]
            return True
        return False
