import networkx as nx
from typing import Dict, List, Any, Optional


class GovernancePolicyEngine:
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
        self.policies = {}

    def register_policy(
        self,
        policy_id: str,
        policy_type: str,
        rules: Dict[str, Any],
        scope: Optional[List[str]] = None
    ) -> None:
        self.policies[policy_id] = {
            "type": policy_type,
            "rules": rules,
            "scope": scope or []
        }

    def overlay_policies(
        self,
        asset_ids: List[str]
    ) -> Dict[str, Any]:
        overlay = {}
        for asset_id in asset_ids:
            if asset_id not in self.graph:
                continue
            applicable = self._get_applicable_policies(asset_id)
            violations = self._check_violations(asset_id, applicable)
            overlay[asset_id] = {
                "policies": applicable,
                "violations": violations,
                "compliance_score": self._compute_compliance_score(violations)
            }
        return overlay

    def _get_applicable_policies(self, asset_id: str) -> List[Dict[str, Any]]:
        node_data = self.graph.nodes[asset_id]
        tags = set(node_data.get("tags", []))
        applicable = []

        for policy_id, policy in self.policies.items():
            if self._is_policy_applicable(asset_id, tags, policy):
                applicable.append({
                    "id": policy_id,
                    "type": policy["type"],
                    "rules": policy["rules"]
                })
        return applicable

    def _is_policy_applicable(
        self,
        asset_id: str,
        tags: set,
        policy: Dict[str, Any]
    ) -> bool:
        if policy["scope"] and asset_id not in policy["scope"]:
            return False
        if policy["type"] == "data_privacy" and "PII" in tags:
            return True
        if policy["type"] == "data_quality":
            return True
        if policy["type"] == "retention" and "archive" in tags:
            return True
        return False

    def _check_violations(
        self,
        asset_id: str,
        policies: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        violations = []
        node_data = self.graph.nodes[asset_id]

        for policy in policies:
            rules = policy["rules"]
            if "encryption_required" in rules:
                if not node_data.get("encrypted", False):
                    violations.append({
                        "policy_id": policy["id"],
                        "rule": "encryption_required",
                        "severity": "high"
                    })
            if "owner_required" in rules:
                if node_data.get("owner") == "unassigned":
                    violations.append({
                        "policy_id": policy["id"],
                        "rule": "owner_required",
                        "severity": "medium"
                    })
        return violations

    def _compute_compliance_score(self, violations: List[Dict[str, str]]) -> float:
        if not violations:
            return 100.0
        penalty = sum(20 if v["severity"] == "high" else 10 for v in violations)
        return max(0.0, 100.0 - penalty)
