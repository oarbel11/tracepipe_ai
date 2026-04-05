from typing import Dict, List, Optional
from dataclasses import dataclass, field

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
