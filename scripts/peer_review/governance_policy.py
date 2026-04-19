from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class GovernancePolicy:
    """Represents a governance policy that can be applied to assets."""
    policy_id: str
    name: str
    description: str
    tags: List[str] = field(default_factory=list)
    rules: Dict[str, str] = field(default_factory=dict)
    severity: str = "medium"
    applies_to: List[str] = field(default_factory=list)

    def matches_asset(self, asset_tags: List[str], asset_id: str) -> bool:
        """Check if policy applies to an asset based on tags or ID."""
        if self.applies_to and asset_id not in self.applies_to:
            return False
        if self.tags:
            return any(tag in asset_tags for tag in self.tags)
        return not self.applies_to

    def check_compliance(self, asset: Dict) -> Optional['PolicyViolation']:
        """Check if asset complies with this policy."""
        asset_id = asset.get('id', '')
        asset_tags = asset.get('tags', [])
        
        if not self.matches_asset(asset_tags, asset_id):
            return None
        
        violations = []
        for rule_key, rule_value in self.rules.items():
            asset_value = str(asset.get(rule_key, '')).lower()
            if rule_value.lower() == 'true' and asset_value != 'true':
                violations.append(f"Missing required: {rule_key}")
            elif rule_value.lower() == 'false' and asset_value == 'true':
                violations.append(f"Prohibited: {rule_key}")
        
        if violations:
            return PolicyViolation(
                policy=self,
                asset_id=asset_id,
                violations=violations,
                timestamp=datetime.now().isoformat()
            )
        return None

    def to_dict(self) -> Dict:
        """Convert policy to dictionary."""
        return {
            "policy_id": self.policy_id,
            "name": self.name,
            "description": self.description,
            "tags": self.tags,
            "rules": self.rules,
            "severity": self.severity,
            "applies_to": self.applies_to
        }

@dataclass
class PolicyViolation:
    """Represents a policy violation detected on an asset."""
    policy: GovernancePolicy
    asset_id: str
    violations: List[str]
    timestamp: str
    remediation_status: str = "pending"

    def to_dict(self) -> Dict:
        return {
            "policy_id": self.policy.policy_id,
            "policy_name": self.policy.name,
            "asset_id": self.asset_id,
            "violations": self.violations,
            "severity": self.policy.severity,
            "timestamp": self.timestamp,
            "remediation_status": self.remediation_status
        }
