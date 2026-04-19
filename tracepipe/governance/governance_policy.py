from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum


class PolicySeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class GovernancePolicy:
    policy_id: str
    name: str
    description: str = ""
    tags: List[str] = field(default_factory=list)
    constraints: Dict[str, any] = field(default_factory=dict)
    severity: PolicySeverity = PolicySeverity.MEDIUM
    auto_remediate: bool = False
    notification_channels: List[str] = field(default_factory=list)

    def matches_asset(self, asset_tags: List[str]) -> bool:
        if not self.tags:
            return False
        return any(tag in asset_tags for tag in self.tags)


@dataclass
class PolicyViolation:
    violation_id: str
    policy: GovernancePolicy
    asset_id: str
    asset_name: str
    description: str
    severity: PolicySeverity
    detected_at: str
    remediation_suggestions: List[str] = field(default_factory=list)
    auto_remediable: bool = False


@dataclass
class RemediationAction:
    action_id: str
    violation_id: str
    action_type: str
    description: str
    status: str = "pending"
    metadata: Dict[str, any] = field(default_factory=dict)
