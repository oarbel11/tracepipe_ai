import yaml
import re
from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime, timedelta

@dataclass
class PolicyViolation:
    asset_id: str
    policy_name: str
    severity: str
    message: str
    detected_at: datetime
    metadata: Dict[str, Any]

class GovernancePolicyEngine:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.policies = yaml.safe_load(f)
    
    def check_asset(self, asset_id: str, asset_metadata: Dict = None) -> List[PolicyViolation]:
        violations = []
        asset_metadata = asset_metadata or {}
        
        for policy in self.policies.get('policies', []):
            if not self._matches_scope(asset_id, policy.get('scope', [])):
                continue
            
            violation = self._evaluate_policy(asset_id, policy, asset_metadata)
            if violation:
                violations.append(violation)
        
        return violations
    
    def _matches_scope(self, asset_id: str, scopes: List[str]) -> bool:
        if not scopes or '*' in scopes:
            return True
        return any(re.match(scope.replace('*', '.*'), asset_id) for scope in scopes)
    
    def _evaluate_policy(self, asset_id: str, policy: Dict, metadata: Dict) -> PolicyViolation:
        rule_type = policy.get('type')
        
        if rule_type == 'schema_drift':
            if metadata.get('schema_changed', False):
                return PolicyViolation(
                    asset_id=asset_id,
                    policy_name=policy['name'],
                    severity=policy.get('severity', 'medium'),
                    message=f"Schema drift detected in {asset_id}",
                    detected_at=datetime.now(),
                    metadata={'old_schema': metadata.get('old_schema'), 'new_schema': metadata.get('new_schema')}
                )
        
        elif rule_type == 'freshness':
            max_age_hours = policy.get('max_age_hours', 24)
            last_updated = metadata.get('last_updated')
            if last_updated and (datetime.now() - last_updated) > timedelta(hours=max_age_hours):
                return PolicyViolation(
                    asset_id=asset_id,
                    policy_name=policy['name'],
                    severity=policy.get('severity', 'high'),
                    message=f"Data freshness violation: {asset_id} not updated in {max_age_hours}h",
                    detected_at=datetime.now(),
                    metadata={'last_updated': last_updated.isoformat()}
                )
        
        elif rule_type == 'pii_detection':
            pii_patterns = policy.get('patterns', [])
            columns = metadata.get('columns', [])
            for col in columns:
                for pattern in pii_patterns:
                    if re.search(pattern, col, re.IGNORECASE):
                        return PolicyViolation(
                            asset_id=asset_id,
                            policy_name=policy['name'],
                            severity=policy.get('severity', 'critical'),
                            message=f"PII detected in column {col} of {asset_id}",
                            detected_at=datetime.now(),
                            metadata={'column': col, 'pattern': pattern}
                        )
        
        return None
