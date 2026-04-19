from typing import Dict, List, Optional
from dataclasses import dataclass, field

@dataclass
class GovernancePolicy:
    policy_id: str
    name: str
    description: str
    tags: List[str] = field(default_factory=list)
    rules: Dict[str, str] = field(default_factory=dict)
    severity: str = "medium"
    applies_to: List[str] = field(default_factory=list)
    enabled: bool = True

    def matches_asset(self, asset_tags: List[str], asset_id: str) -> bool:
        if not self.enabled:
            return False
        if self.applies_to and asset_id not in self.applies_to:
            return False
        if self.tags:
            return any(tag in asset_tags for tag in self.tags)
        return not self.applies_to

    def to_dict(self) -> Dict:
        return {
            "policy_id": self.policy_id,
            "name": self.name,
            "description": self.description,
            "tags": self.tags,
            "rules": self.rules,
            "severity": self.severity,
            "applies_to": self.applies_to,
            "enabled": self.enabled
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'GovernancePolicy':
        return cls(
            policy_id=data['policy_id'],
            name=data['name'],
            description=data['description'],
            tags=data.get('tags', []),
            rules=data.get('rules', {}),
            severity=data.get('severity', 'medium'),
            applies_to=data.get('applies_to', []),
            enabled=data.get('enabled', True)
        )

    def add_schema_rule(self, required_columns: List[str]):
        self.rules['required_columns'] = ','.join(required_columns)

    def add_quality_rule(self, rule_name: str, threshold: float):
        self.rules[rule_name] = str(threshold)

    def validate_schema(self, actual_columns: List[str]) -> List[str]:
        if 'required_columns' not in self.rules:
            return []
        required = [c.strip() for c in self.rules['required_columns'].split(',')]
        return [c for c in required if c not in actual_columns]

    def validate_quality(self, metrics: Dict[str, float]) -> Dict[str, float]:
        violations = {}
        for rule_name, threshold_str in self.rules.items():
            if rule_name == 'required_columns':
                continue
            try:
                threshold = float(threshold_str)
                if rule_name in metrics and metrics[rule_name] > threshold:
                    violations[rule_name] = metrics[rule_name]
            except ValueError:
                continue
        return violations
