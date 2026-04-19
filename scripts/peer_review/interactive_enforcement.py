from typing import Dict, List, Optional
from scripts.peer_review.governance_policy import GovernancePolicy, PolicyViolation
from scripts.peer_review.policy_enforcement import PolicyViolationDetector, EnforcementEngine

class InteractiveLineageEnforcement:
    """Interactive governance enforcement for lineage graphs."""
    
    def __init__(self, policies: List[GovernancePolicy]):
        self.detector = PolicyViolationDetector(policies)
        self.engine = EnforcementEngine()
    
    def analyze_lineage_graph(self, assets: List[Dict], 
                             upstream_changes: Optional[Dict[str, List[str]]] = None) -> Dict:
        """Analyze lineage graph for violations and high-risk assets."""
        violations = self.detector.detect_violations(assets)
        high_risk = []
        if upstream_changes:
            high_risk = self.detector.get_high_risk_assets(assets, upstream_changes)
        
        return {
            'violations': [v.to_dict() for v in violations],
            'high_risk_assets': high_risk,
            'total_violations': len(violations),
            'critical_violations': sum(1 for v in violations if v.policy.severity == 'high')
        }
    
    def get_visualization_highlights(self, assets: List[Dict]) -> Dict:
        """Get visual highlighting data for interactive UI."""
        violations = self.detector.detect_violations(assets)
        highlights = {'violation_nodes': [], 'severity_map': {}}
        
        for violation in violations:
            highlights['violation_nodes'].append(violation.asset_id)
            highlights['severity_map'][violation.asset_id] = violation.policy.severity
        
        return highlights
    
    def initiate_enforcement_actions(self, assets: List[Dict]) -> Dict:
        """Initiate enforcement actions for all violations."""
        violations = self.detector.detect_violations(assets)
        enforcement_results = self.engine.enforce_violations(violations)
        
        return {
            'enforcement_summary': enforcement_results,
            'total_alerts': len(enforcement_results['alerts']),
            'assets_with_remediations': len(enforcement_results['remediations'])
        }
    
    def get_compliance_dashboard(self, assets: List[Dict]) -> Dict:
        """Generate compliance dashboard data."""
        violations = self.detector.detect_violations(assets)
        
        severity_counts = {'high': 0, 'medium': 0, 'low': 0}
        for v in violations:
            severity_counts[v.policy.severity] = severity_counts.get(v.policy.severity, 0) + 1
        
        compliance_rate = ((len(assets) - len(violations)) / len(assets) * 100) if assets else 100
        
        return {
            'total_assets': len(assets),
            'compliant_assets': len(assets) - len(violations),
            'violations_by_severity': severity_counts,
            'compliance_rate': round(compliance_rate, 2),
            'requires_immediate_action': severity_counts['high']
        }
