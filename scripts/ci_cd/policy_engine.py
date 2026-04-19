from typing import Dict, List
import yaml
import re
from scripts.peer_review.governance_policy import GovernancePolicy

class PolicyEngine:
    def __init__(self, config_path: str):
        self.policies = self._load_policies(config_path)
        self.rule_handlers = {
            'pii_detection': self._check_pii,
            'schema_breaking': self._check_schema,
            'performance_threshold': self._check_performance,
            'data_contract': self._check_contract
        }
    
    def _load_policies(self, config_path: str) -> List[GovernancePolicy]:
        try:
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f) or {}
                policies_data = data.get('policies', [])
                return [GovernancePolicy(**p) for p in policies_data]
        except FileNotFoundError:
            return self._default_policies()
    
    def _default_policies(self) -> List[GovernancePolicy]:
        return [
            GovernancePolicy(
                policy_id='pii-block',
                name='PII Detection',
                description='Block PRs exposing PII',
                rules={'pii_detection': 'true'},
                severity='critical'
            ),
            GovernancePolicy(
                policy_id='schema-check',
                name='Schema Evolution',
                description='Validate schema changes',
                rules={'schema_breaking': 'true'},
                severity='high'
            )
        ]
    
    def evaluate(self, pr_data: Dict) -> List[Dict]:
        violations = []
        for policy in self.policies:
            for rule_type, rule_value in policy.rules.items():
                handler = self.rule_handlers.get(rule_type)
                if handler:
                    result = handler(pr_data, rule_value)
                    if result:
                        violations.append({
                            'policy_id': policy.policy_id,
                            'name': policy.name,
                            'severity': policy.severity,
                            'message': result,
                            'rule_type': rule_type
                        })
        return violations
    
    def _check_pii(self, pr_data: Dict, rule_value: str) -> Optional[str]:
        pii_patterns = [r'ssn', r'social.security', r'credit.card', r'email.*address']
        files = pr_data.get('files', [])
        for pattern in pii_patterns:
            if any(re.search(pattern, str(f), re.I) for f in files):
                return f"Potential PII exposure detected: {pattern}"
        return None
    
    def _check_schema(self, pr_data: Dict, rule_value: str) -> Optional[str]:
        title = pr_data.get('title', '').lower()
        if 'drop' in title or 'delete column' in title:
            return "Breaking schema change detected"
        return None
    
    def _check_performance(self, pr_data: Dict, rule_value: str) -> Optional[str]:
        return None
    
    def _check_contract(self, pr_data: Dict, rule_value: str) -> Optional[str]:
        return None

from typing import Optional
