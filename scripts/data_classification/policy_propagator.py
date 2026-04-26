from typing import Dict, List, Set
from scripts.data_classification.classifier import Classification, SensitivityLevel

class PolicyPropagator:
    def __init__(self, lineage_graph: Dict[str, List[str]]):
        self.lineage_graph = lineage_graph
        self.policies = {}

    def _traverse_downstream(self, node: str, visited: Set[str] = None) -> List[str]:
        if visited is None:
            visited = set()
        if node in visited:
            return []
        visited.add(node)
        downstream = [node]
        for child in self.lineage_graph.get(node, []):
            downstream.extend(self._traverse_downstream(child, visited))
        return downstream

    def propagate_classification(self, source_asset: str,
                                classification: Classification) -> Dict[str, Dict]:
        downstream_assets = self._traverse_downstream(source_asset)
        propagated = {}
        for asset in downstream_assets:
            policy = {
                'asset': asset,
                'column': classification.column_name,
                'sensitivity_level': classification.sensitivity_level.value,
                'access_control': self._determine_access_control(
                    classification.sensitivity_level
                ),
                'masking_rule': self._determine_masking_rule(
                    classification.sensitivity_level
                )
            }
            propagated[asset] = policy
            self.policies[f"{asset}.{classification.column_name}"] = policy
        return propagated

    def _determine_access_control(self, level: SensitivityLevel) -> str:
        if level == SensitivityLevel.RESTRICTED:
            return "DENY_ALL"
        elif level == SensitivityLevel.CONFIDENTIAL:
            return "REQUIRE_APPROVAL"
        elif level == SensitivityLevel.INTERNAL:
            return "AUTHENTICATED_ONLY"
        return "PUBLIC"

    def _determine_masking_rule(self, level: SensitivityLevel) -> str:
        if level == SensitivityLevel.RESTRICTED:
            return "FULL_MASK"
        elif level == SensitivityLevel.CONFIDENTIAL:
            return "PARTIAL_MASK"
        return "NONE"

    def get_policies(self) -> Dict[str, Dict]:
        return self.policies
