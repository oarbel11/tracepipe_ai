import pytest
from src.cicd_integration import ImpactAnalyzer, PolicyEnforcer, GitCICDIntegration


class TestImpactAnalyzer:
    def test_analyze_changes(self):
        analyzer = ImpactAnalyzer()
        result = analyzer.analyze_changes({'files': ['file1.py', 'file2.py']})
        assert 'impact_score' in result
        assert 'affected_pipelines' in result
        assert 'risk_level' in result


class TestPolicyEnforcer:
    def test_enforce_policies_no_violations(self):
        enforcer = PolicyEnforcer()
        result = enforcer.enforce_policies({'impact_score': 30})
        assert result['passed'] is True
        assert len(result['violations']) == 0

    def test_enforce_policies_with_high_severity(self):
        enforcer = PolicyEnforcer()
        result = enforcer.enforce_policies({'impact_score': 90})
        assert result['passed'] is False
        assert len(result['violations']) > 0
        assert result['violations'][0]['severity'] == 'high'
