from typing import Dict, List, Optional, Callable
from scripts.lineage_graph_store import LineageGraphStore
from scripts.peer_review.policy_engine import PolicyEngine, PolicyViolation
from scripts.peer_review.governance_policy import GovernancePolicy
import json
import time

class GovernanceOrchestrator:
    def __init__(self, db_path: str = 'lineage.duckdb'):
        self.graph_store = LineageGraphStore(db_path)
        self.policy_engine = PolicyEngine()
        self.remediation_handlers: Dict[str, Callable] = {
            'mask': self._mask_handler,
            'alert': self._alert_handler,
            'quarantine': self._quarantine_handler
        }
        self.violation_log: List[Dict] = []

    def add_policy(self, policy: GovernancePolicy):
        self.policy_engine.add_policy(policy)

    def register_asset(self, asset_id: str, tags: List[str], metadata: Dict, node_type: str = 'table'):
        self.graph_store.add_node(asset_id, node_type, tags, metadata)

    def register_lineage(self, source_id: str, target_id: str, edge_type: str = 'produces'):
        self.graph_store.add_edge(source_id, target_id, edge_type)

    def evaluate_policies(self, asset_id: Optional[str] = None) -> List[PolicyViolation]:
        all_violations = []
        if asset_id:
            nodes = [n for n in self.graph_store.find_nodes_by_tags([]) if n['node_id'] == asset_id]
        else:
            nodes = self.graph_store.find_nodes_by_tags([])
        
        for node in nodes:
            violations = self.policy_engine.evaluate_asset(
                node['node_id'], node['tags'], node['metadata']
            )
            all_violations.extend(violations)
        
        return all_violations

    def execute_remediation(self, violation: PolicyViolation, action: str) -> Dict:
        if action not in self.remediation_handlers:
            return {'status': 'error', 'message': f'Unknown action: {action}'}
        
        result = self.remediation_handlers[action](violation)
        self.violation_log.append({
            'timestamp': time.time(),
            'violation': violation.to_dict(),
            'action': action,
            'result': result
        })
        return result

    def _mask_handler(self, violation: PolicyViolation) -> Dict:
        return {'status': 'success', 'action': 'mask', 'asset': violation.asset_id, 'message': 'Data masked'}

    def _alert_handler(self, violation: PolicyViolation) -> Dict:
        return {'status': 'success', 'action': 'alert', 'severity': violation.severity, 'message': 'Alert sent'}

    def _quarantine_handler(self, violation: PolicyViolation) -> Dict:
        return {'status': 'success', 'action': 'quarantine', 'asset': violation.asset_id, 'message': 'Asset quarantined'}

    def get_violation_log(self) -> List[Dict]:
        return self.violation_log

    def close(self):
        self.graph_store.close()
