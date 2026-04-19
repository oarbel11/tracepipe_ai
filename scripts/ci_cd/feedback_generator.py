from typing import Dict, List

class FeedbackGenerator:
    def __init__(self):
        self.suggestion_templates = {
            'pii_detection': self._pii_suggestions,
            'schema_breaking': self._schema_suggestions,
            'performance_threshold': self._performance_suggestions,
            'data_contract': self._contract_suggestions
        }
    
    def generate(self, violations: List[Dict], pr_data: Dict) -> Dict:
        if not violations:
            return {
                'summary': '✅ All governance policies passed',
                'details': 'No violations detected',
                'suggestions': []
            }
        
        critical = [v for v in violations if v['severity'] == 'critical']
        high = [v for v in violations if v['severity'] == 'high']
        
        suggestions = []
        for violation in violations:
            rule_type = violation.get('rule_type')
            handler = self.suggestion_templates.get(rule_type)
            if handler:
                suggestions.extend(handler(violation, pr_data))
        
        summary = f"❌ {len(violations)} policy violation(s) found"
        if critical:
            summary += f" ({len(critical)} critical - merge blocked)"
        
        return {
            'summary': summary,
            'details': self._format_violations(violations),
            'suggestions': suggestions,
            'can_merge': len(critical) == 0
        }
    
    def _format_violations(self, violations: List[Dict]) -> str:
        lines = []
        for v in violations:
            emoji = '🔴' if v['severity'] == 'critical' else '🟡'
            lines.append(f"{emoji} [{v['severity'].upper()}] {v['name']}: {v['message']}")
        return '\n'.join(lines)
    
    def _pii_suggestions(self, violation: Dict, pr_data: Dict) -> List[str]:
        return [
            "Ensure PII fields are properly masked or encrypted",
            "Consider using a data masking function for sensitive columns",
            "Review data classification tags on affected tables"
        ]
    
    def _schema_suggestions(self, violation: Dict, pr_data: Dict) -> List[str]:
        return [
            "Use schema evolution with backward compatibility",
            "Add a migration script for existing data",
            "Consider deprecating the column instead of dropping"
        ]
    
    def _performance_suggestions(self, violation: Dict, pr_data: Dict) -> List[str]:
        return [
            "Add partition pruning to reduce scan size",
            "Consider caching intermediate results",
            "Use broadcast join for small dimension tables"
        ]
    
    def _contract_suggestions(self, violation: Dict, pr_data: Dict) -> List[str]:
        return [
            "Update the data contract definition",
            "Notify downstream consumers of the change",
            "Version the API endpoint to maintain compatibility"
        ]
