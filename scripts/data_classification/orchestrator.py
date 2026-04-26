from typing import Dict, List
from scripts.data_classification.classifier import SensitiveDataClassifier
from scripts.data_classification.policy_propagator import PolicyPropagator

class ClassificationOrchestrator:
    def __init__(self, lineage_graph: Dict[str, List[str]]):
        self.classifier = SensitiveDataClassifier()
        self.propagator = PolicyPropagator(lineage_graph)
        self.classifications = {}

    def classify_and_propagate(self, asset: str,
                              columns: Dict[str, Dict]) -> Dict[str, Dict]:
        classifications = self.classifier.classify_table(columns)
        self.classifications[asset] = classifications
        all_policies = {}
        for classification in classifications:
            if classification.sensitivity_level.value != "public":
                policies = self.propagator.propagate_classification(
                    asset, classification
                )
                all_policies.update(policies)
        return all_policies

    def get_all_classifications(self) -> Dict[str, List]:
        return self.classifications

    def get_all_policies(self) -> Dict[str, Dict]:
        return self.propagator.get_policies()

    def generate_compliance_report(self) -> Dict:
        total_columns = 0
        sensitive_columns = 0
        by_level = {}
        for asset, classifications in self.classifications.items():
            for classification in classifications:
                total_columns += 1
                level = classification.sensitivity_level.value
                by_level[level] = by_level.get(level, 0) + 1
                if level != "public":
                    sensitive_columns += 1
        return {
            'total_columns': total_columns,
            'sensitive_columns': sensitive_columns,
            'by_sensitivity_level': by_level,
            'total_policies': len(self.propagator.get_policies())
        }
