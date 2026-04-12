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
    enforcement_enabled: bool = True

    def matches_asset(self, asset_tags: List[str], asset_id: str) -> bool:
        if self.applies_to and asset_id not in self.applies_to:
            return False
        if self.tags:
            return any(tag in asset_tags for tag in self.tags)
        return not self.applies_to

    def get_enforcement_action(self) -> str:
        return self.rules.get('action', 'monitor')

    def get_access_level(self) -> Optional[str]:
        return self.rules.get('access_level')

    def is_enforced(self) -> bool:
        return self.enforcement_enabled and self.get_enforcement_action() != 'monitor'

    def to_dict(self) -> Dict:
        return {
            "policy_id": self.policy_id,
            "name": self.name,
            "description": self.description,
            "tags": self.tags,
            "rules": self.rules,
            "severity": self.severity,
            "applies_to": self.applies_to,
            "enforcement_enabled": self.enforcement_enabled
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
            enforcement_enabled=data.get('enforcement_enabled', True)
        )

    def validate_rules(self) -> List[str]:
        errors = []
        valid_actions = ['monitor', 'propagate', 'restrict', 'alert']
        action = self.rules.get('action')
        if action and action not in valid_actions:
            errors.append(f"Invalid action: {action}")
        return errors
