from scripts.lineage_graph_store import LineageGraphStore
from scripts.peer_review.policy_engine import PolicyEngine

class GovernanceOrchestrator:
    def __init__(self, graph_store=None):
        self.graph_store = graph_store or LineageGraphStore()
        self.policy_engine = PolicyEngine(self.graph_store)
        self.remediation_log = []
        self.alert_log = []

    def add_policy(self, policy):
        self.policy_engine.add_policy(policy)

    def scan_and_remediate(self):
        violations = self.policy_engine.evaluate_policies()
        results = []
        
        for violation in violations:
            remediation_type = violation.get('remediation', 'alert')
            
            if remediation_type == 'mask':
                result = self._apply_masking(violation)
            elif remediation_type == 'alert':
                result = self._send_alert(violation)
            elif remediation_type == 'quarantine':
                result = self._quarantine_data(violation)
            else:
                result = self._send_alert(violation)
            
            results.append(result)
        
        return results

    def _apply_masking(self, violation):
        node_id = violation['node_id']
        action = {
            'action': 'mask',
            'node_id': node_id,
            'status': 'success',
            'message': f"Applied masking to {node_id}"
        }
        self.remediation_log.append(action)
        return action

    def _send_alert(self, violation):
        alert = {
            'action': 'alert',
            'node_id': violation['node_id'],
            'severity': violation['severity'],
            'message': violation['message'],
            'status': 'sent'
        }
        self.alert_log.append(alert)
        return alert

    def _quarantine_data(self, violation):
        node_id = violation['node_id']
        action = {
            'action': 'quarantine',
            'node_id': node_id,
            'status': 'success',
            'message': f"Quarantined {node_id}"
        }
        self.remediation_log.append(action)
        return action

    def get_remediation_log(self):
        return self.remediation_log

    def get_alert_log(self):
        return self.alert_log
