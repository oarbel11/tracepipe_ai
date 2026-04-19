from typing import Dict, List, Optional
import json
from datetime import datetime
from scripts.peer_review.governance_policy import GovernancePolicy
from scripts.anomaly_detector import AnomalyDetector

class GovernanceEngine:
    def __init__(self):
        self.policies: List[GovernancePolicy] = []
        self.anomaly_detector = AnomalyDetector()
        self.violations: List[Dict] = []
        self.baseline_schemas: Dict[str, Dict] = {}

    def add_policy(self, policy: GovernancePolicy):
        self.policies.append(policy)

    def load_policies_from_dict(self, policies_data: List[Dict]):
        for p in policies_data:
            policy = GovernancePolicy(**p)
            self.add_policy(policy)

    def set_baseline_schema(self, asset_id: str, schema: Dict):
        self.baseline_schemas[asset_id] = schema

    def check_schema_drift(self, asset_id: str, current_schema: Dict) -> List[Dict]:
        violations = []
        if asset_id not in self.baseline_schemas:
            return violations
        
        baseline = self.baseline_schemas[asset_id]
        drift = self.anomaly_detector.detect_schema_changes(baseline, current_schema)
        
        if drift['added_columns'] or drift['removed_columns'] or drift['type_changes']:
            violations.append({
                'asset_id': asset_id,
                'violation_type': 'schema_drift',
                'severity': 'high',
                'details': drift,
                'timestamp': datetime.now().isoformat()
            })
        return violations

    def check_policy_compliance(self, asset_id: str, asset_tags: List[str], 
                                metadata: Dict) -> List[Dict]:
        violations = []
        for policy in self.policies:
            if not policy.matches_asset(asset_tags, asset_id):
                continue
            
            policy_violations = self._validate_rules(asset_id, policy, metadata)
            violations.extend(policy_violations)
        
        return violations

    def _validate_rules(self, asset_id: str, policy: GovernancePolicy, 
                       metadata: Dict) -> List[Dict]:
        violations = []
        rules = policy.rules
        
        if 'required_columns' in rules:
            required = [c.strip() for c in rules['required_columns'].split(',')]
            actual = metadata.get('columns', [])
            missing = [c for c in required if c not in actual]
            if missing:
                violations.append({
                    'asset_id': asset_id,
                    'policy_id': policy.policy_id,
                    'violation_type': 'missing_required_columns',
                    'severity': policy.severity,
                    'details': {'missing': missing},
                    'timestamp': datetime.now().isoformat()
                })
        
        if 'max_null_rate' in rules:
            max_null = float(rules['max_null_rate'])
            null_rates = metadata.get('null_rates', {})
            high_null_cols = {col: rate for col, rate in null_rates.items() 
                            if rate > max_null}
            if high_null_cols:
                violations.append({
                    'asset_id': asset_id,
                    'policy_id': policy.policy_id,
                    'violation_type': 'high_null_rate',
                    'severity': policy.severity,
                    'details': {'columns': high_null_cols},
                    'timestamp': datetime.now().isoformat()
                })
        
        return violations

    def generate_report(self) -> Dict:
        return {
            'total_violations': len(self.violations),
            'by_severity': self._group_by_severity(),
            'violations': self.violations,
            'generated_at': datetime.now().isoformat()
        }

    def _group_by_severity(self) -> Dict[str, int]:
        counts = {'high': 0, 'medium': 0, 'low': 0}
        for v in self.violations:
            severity = v.get('severity', 'medium')
            counts[severity] = counts.get(severity, 0) + 1
        return counts
