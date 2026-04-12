class InteractiveImpactAnalyzer:
    def __init__(self, config=None):
        self.config = config or {}
        self.last_analysis = None

    def analyze_changes(self, changes):
        if isinstance(changes, str):
            changes = {'files': [changes]}
        elif not isinstance(changes, dict):
            changes = {'files': []}
        
        files = changes.get('files', [])
        impact_score = min(len(files) * 10, 100)
        
        self.last_analysis = {
            'impact_score': impact_score,
            'affected_pipelines': [f'pipeline_{i}' for i in range(len(files))],
            'risk_level': 'high' if impact_score > 50 else 'low',
            'details': f'Analyzed {len(files)} file(s)'
        }
        
        return self.last_analysis

    def get_report(self):
        if not self.last_analysis:
            return {'status': 'no_analysis', 'report': ''}
        
        report = f"Impact Score: {self.last_analysis['impact_score']}\n"
        report += f"Risk Level: {self.last_analysis['risk_level']}\n"
        report += f"Details: {self.last_analysis['details']}"
        
        return {
            'status': 'success',
            'report': report
        }
