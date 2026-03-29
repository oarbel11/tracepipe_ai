"""Governance policy engine for Tracepipe AI."""
from typing import List, Dict
from scripts.peer_review.lineage_graph import LineageGraph

class GovernancePolicy:
    def __init__(self, policy_id: str, policy_type: str, rules: Dict, affected_tags: List[str]):
        self.policy_id = policy_id
        self.policy_type = policy_type
        self.rules = rules
        self.affected_tags = affected_tags

class PolicyViolation:
    def __init__(self, node_id: str, policy_id: str, violation_type: str, severity: str):
        self.node_id = node_id
        self.policy_id = policy_id
        self.violation_type = violation_type
        self.severity = severity

class GovernancePolicyEngine:
    def __init__(self, lineage_graph: LineageGraph):
        self.graph = lineage_graph
        self.policies = {}

    def add_policy(self, policy: GovernancePolicy):
        self.policies[policy.policy_id] = policy

    def get_applicable_policies(self, node_id: str) -> List[GovernancePolicy]:
        node = self.graph.get_node(node_id)
        if not node:
            return []
        applicable = []
        for policy in self.policies.values():
            if any(tag in node.tags for tag in policy.affected_tags):
                applicable.append(policy)
        return applicable

    def check_violations(self, node_id: str) -> List[PolicyViolation]:
        violations = []
        policies = self.get_applicable_policies(node_id)
        node = self.graph.get_node(node_id)
        if not node:
            return violations
        for policy in policies:
            if policy.policy_type == 'data_retention':
                if 'retention_days' not in node.metadata:
                    violations.append(PolicyViolation(node_id, policy.policy_id, 'missing_retention', 'high'))
        return violations
