from typing import Dict, List, Any, Optional
from .policy_engine import Policy, PolicyType
import re

class Violation:
    def __init__(self, policy_id: str, asset_id: str, violation_type: str,
                 details: Dict[str, Any]):
        self.policy_id = policy_id
        self.asset_id = asset_id
        self.violation_type = violation_type
        self.details = details

class ViolationDetector:
    def __init__(self):
        self.pii_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'
        }

    def check_schema_change(self, policy: Policy, current_schema: List[str],
                            previous_schema: List[str]) -> Optional[Violation]:
        if current_schema != previous_schema:
            return Violation(policy.policy_id, policy.asset_id,
                             'schema_change',
                             {'current': current_schema,
                              'previous': previous_schema})
        return None

    def check_freshness(self, policy: Policy,
                        last_update_hours: float) -> Optional[Violation]:
        threshold = policy.rules.get('max_hours', 24)
        if last_update_hours > threshold:
            return Violation(policy.policy_id, policy.asset_id, 'freshness',
                             {'last_update_hours': last_update_hours,
                              'threshold': threshold})
        return None

    def check_pii(self, policy: Policy, data_sample: str) -> Optional[Violation]:
        detected_pii = []
        for pii_type, pattern in self.pii_patterns.items():
            if re.search(pattern, data_sample):
                detected_pii.append(pii_type)
        if detected_pii:
            return Violation(policy.policy_id, policy.asset_id,
                             'pii_detection', {'detected': detected_pii})
        return None

    def detect_violations(self, policy: Policy,
                          asset_data: Dict[str, Any]) -> Optional[Violation]:
        if policy.policy_type == PolicyType.SCHEMA_CHANGE:
            return self.check_schema_change(
                policy, asset_data.get('current_schema', []),
                asset_data.get('previous_schema', []))
        elif policy.policy_type == PolicyType.FRESHNESS:
            return self.check_freshness(
                policy, asset_data.get('last_update_hours', 0))
        elif policy.policy_type == PolicyType.PII_DETECTION:
            return self.check_pii(policy, asset_data.get('data_sample', ''))
        return None
