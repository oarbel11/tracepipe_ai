class ImpactAnalyzer:
    def __init__(self, config=None):
        self.config = config or {}

    def analyze_changes(self, changes):
        if isinstance(changes, str):
            changes = {'files': [changes]}
        elif not isinstance(changes, dict):
            changes = {'files': []}
        
        files = changes.get('files', [])
        impact_score = min(len(files) * 10, 100)
        
        return {
            'impact_score': impact_score,
            'affected_pipelines': [f'pipeline_{i}' for i in range(len(files))],
            'risk_level': 'high' if impact_score > 50 else 'low'
        }


class PolicyEnforcer:
    def __init__(self, config=None):
        self.config = config or {}

    def enforce_policies(self, analysis_result):
        if isinstance(analysis_result, str):
            analysis_result = {'impact_score': 0}
        elif not isinstance(analysis_result, dict):
            analysis_result = {'impact_score': 0}
        
        violations = []
        impact_score = analysis_result.get('impact_score', 0)
        
        if impact_score > 80:
            violations.append({
                'severity': 'high',
                'message': 'High impact change detected'
            })
        
        return {
            'violations': violations,
            'passed': len(violations) == 0
        }


class GitCICDIntegration:
    def __init__(self, config=None):
        self.config = config or {}
        self.analyzer = ImpactAnalyzer(self.config)
        self.enforcer = PolicyEnforcer(self.config)

    def process_commit(self, commit_data):
        if isinstance(commit_data, str):
            commit_data = {'files': [commit_data]}
        
        analysis = self.analyzer.analyze_changes(commit_data)
        policy_result = self.enforcer.enforce_policies(analysis)
        
        return {
            'status': 'success' if policy_result['passed'] else 'failed',
            'analysis': analysis,
            'policy_result': policy_result
        }

    def webhook_handler(self, event_type, payload):
        if event_type not in ['push', 'pull_request']:
            return {'status': 'ignored'}
        
        return self.process_commit(payload)
