"""Governance policy engine for overlaying compliance policies."""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class PolicyViolation:
    """Represents a policy violation."""
    asset_id: str
    policy_id: str
    policy_name: str
    severity: str
    message: str


@dataclass
class GovernancePolicy:
    """Governance policy definition."""
    policy_id: str
    name: str
    description: str
    rules: Dict[str, Any]
    severity: str


class GovernancePolicyEngine:
    """Engine for overlaying governance policies on lineage."""

    def __init__(self, policies: Optional[List[GovernancePolicy]] = None):
        """Initialize with policies."""
        self.policies = policies or []

    def add_policy(self, policy: GovernancePolicy) -> None:
        """Add a governance policy."""
        self.policies.append(policy)

    def evaluate_lineage(
        self,
        lineage_graph: Dict[str, Any]
    ) -> List[PolicyViolation]:
        """Evaluate policies against lineage graph."""
        violations: List[PolicyViolation] = []
        nodes = lineage_graph.get("nodes", [])

        for node in nodes:
            node_violations = self._check_node_policies(node)
            violations.extend(node_violations)

        return violations

    def _check_node_policies(
        self,
        node: Dict[str, Any]
    ) -> List[PolicyViolation]:
        """Check all policies against a node."""
        violations: List[PolicyViolation] = []
        metadata = node.get("metadata", {})

        for policy in self.policies:
            if self._violates_policy(node, metadata, policy):
                violations.append(PolicyViolation(
                    asset_id=node["id"],
                    policy_id=policy.policy_id,
                    policy_name=policy.name,
                    severity=policy.severity,
                    message=f"Policy '{policy.name}' violated"
                ))

        return violations

    def _violates_policy(
        self,
        node: Dict,
        metadata: Dict,
        policy: GovernancePolicy
    ) -> bool:
        """Check if node violates policy."""
        rules = policy.rules
        if "required_tags" in rules:
            node_tags = set(metadata.get("tags", []))
            required = set(rules["required_tags"])
            if not required.issubset(node_tags):
                return True
        if "forbidden_tags" in rules:
            node_tags = set(metadata.get("tags", []))
            forbidden = set(rules["forbidden_tags"])
            if node_tags.intersection(forbidden):
                return True
        return False
