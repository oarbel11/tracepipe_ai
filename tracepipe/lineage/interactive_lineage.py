from typing import List, Dict, Optional
from tracepipe.governance.policy_enforcement import (
    PolicyViolationDetector,
    EnforcementEngine,
)
from tracepipe.governance.governance_policy import GovernancePolicy


class InteractiveLineageGraph:
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: List[Dict] = []
        self.detector: Optional[PolicyViolationDetector] = None
        self.enforcement: Optional[EnforcementEngine] = None

    def add_node(self, node_id: str, name: str, tags: List[str]):
        self.nodes[node_id] = {"id": node_id, "name": name, "tags": tags}

    def add_edge(self, source: str, target: str):
        self.edges.append({"source": source, "target": target})

    def attach_policy_enforcement(
        self, policies: List[GovernancePolicy]
    ):
        self.detector = PolicyViolationDetector(policies)
        self.enforcement = EnforcementEngine()

    def scan_for_violations(self):
        if not self.detector or not self.enforcement:
            return
        for node_id, node in self.nodes.items():
            violations = self.detector.check_asset(
                node_id, node["name"], node["tags"]
            )
            if violations:
                self.enforcement.process_violations(violations)

    def get_node_visualization_data(self, node_id: str) -> Dict:
        if node_id not in self.nodes:
            return {}
        node = self.nodes[node_id]
        highlight = "normal"
        if self.enforcement:
            high_risk = self.enforcement.get_high_risk_assets()
            if node_id in high_risk:
                highlight = "high_risk"
        return {"id": node_id, "name": node["name"], "highlight": highlight}

    def get_violations_for_node(self, node_id: str) -> List:
        if not self.enforcement:
            return []
        return [
            v
            for v in self.enforcement.violations
            if v.asset_id == node_id
        ]
