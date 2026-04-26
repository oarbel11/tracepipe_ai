from typing import Dict, List, Set, Tuple
import networkx as nx
from scripts.peer_review.governance_policy import GovernancePolicy
from scripts.data_classification.classifier import Classification

class PolicyPropagator:
    def __init__(self):
        self.lineage_graph = nx.DiGraph()
        self.applied_policies: Dict[str, List[GovernancePolicy]] = {}
        self.classifications: Dict[str, List[Classification]] = {}

    def build_lineage_graph(self, lineage_edges: List[Tuple[str, str]]):
        for source, target in lineage_edges:
            self.lineage_graph.add_edge(source, target)

    def create_policy_from_classification(self, classification: Classification) -> GovernancePolicy:
        return GovernancePolicy(
            policy_id=f"policy_{classification.classification_type}_{classification.asset_id}",
            name=f"{classification.classification_type.upper()} Protection",
            description=f"Auto-generated policy for {classification.classification_type} data",
            tags=classification.policy_tags,
            rules={
                'access_control': 'restricted',
                'masking': 'required' if 'sensitive' in classification.policy_tags else 'optional',
                'encryption': 'required' if 'sensitive' in classification.policy_tags else 'recommended'
            },
            severity='high' if 'sensitive' in classification.policy_tags else 'medium',
            applies_to=[classification.asset_id]
        )

    def propagate_downstream(self, source_asset: str, policy: GovernancePolicy):
        if source_asset not in self.lineage_graph:
            return
        descendants = nx.descendants(self.lineage_graph, source_asset)
        for downstream_asset in descendants:
            if downstream_asset not in self.applied_policies:
                self.applied_policies[downstream_asset] = []
            propagated_policy = GovernancePolicy(
                policy_id=f"{policy.policy_id}_propagated_{downstream_asset}",
                name=policy.name,
                description=f"Propagated from {source_asset}: {policy.description}",
                tags=policy.tags,
                rules=policy.rules,
                severity=policy.severity,
                applies_to=[downstream_asset]
            )
            self.applied_policies[downstream_asset].append(propagated_policy)

    def propagate_policies(self, classifications: Dict[str, List[Classification]], lineage_edges: List[Tuple[str, str]] = None):
        self.classifications = classifications
        if lineage_edges:
            self.build_lineage_graph(lineage_edges)
        for asset_id, class_list in classifications.items():
            for classification in class_list:
                policy = self.create_policy_from_classification(classification)
                if asset_id not in self.applied_policies:
                    self.applied_policies[asset_id] = []
                self.applied_policies[asset_id].append(policy)
                self.propagate_downstream(asset_id, policy)

    def get_applied_policies(self) -> Dict[str, List[GovernancePolicy]]:
        return self.applied_policies

    def get_policies_for_asset(self, asset_id: str) -> List[GovernancePolicy]:
        return self.applied_policies.get(asset_id, [])
