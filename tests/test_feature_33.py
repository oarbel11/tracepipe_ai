import pytest
from scripts.peer_review.git_integration import GitCICDIntegration
from scripts.peer_review.impact_analyzer import ImpactAnalyzer, InteractiveImpactAnalyzer
from scripts.peer_review.policy_enforcer import PolicyEnforcer


class TestGitCICDIntegration:
    def test_initialization(self):
        integration = GitCICDIntegration()
        assert integration.impact_analyzer is not None
        assert integration.policy_enforcer is not None

    def test_process_commit_low_risk(self):
        integration = GitCICDIntegration()
        commit = {
            "sha": "abc123",
            "modified": ["README.md"],
            "added": [],
            "removed": []
        }
        result = integration.process_commit(commit)
        assert result["commit_sha"] == "abc123"
        assert result["impact"]["risk_level"] == "low"
        assert result["approved"] is True

    def test_webhook_handling(self):
        integration = GitCICDIntegration()
        payload = {"commits": [{"sha": "xyz", "modified": ["test.py"]}]}
        result = integration.handle_webhook("push", payload)
        assert result["status"] == "processed"


class TestImpactAnalyzer:
    def test_analyze_changes(self):
        analyzer = ImpactAnalyzer()
        changes = [{"file_path": "pipeline/data.sql", "change_type": "modified"}]
        result = analyzer.analyze_changes(changes)
        assert "risk_level" in result
        assert "affected_pipelines" in result

    def test_interactive_analyzer(self):
        analyzer = InteractiveImpactAnalyzer()
        changes = [{"file_path": "schema/model.py", "change_type": "modified"}]
        result = analyzer.analyze_interactive(changes)
        assert result["interactive"] is True


class TestPolicyEnforcer:
    def test_enforce_policies(self):
        analyzer = ImpactAnalyzer()
        enforcer = PolicyEnforcer(policies=[], impact_analyzer=analyzer)
        changes = [{"file_path": "test.py", "change_type": "modified"}]
        result = enforcer.enforce_policies(changes)
        assert "violations" in result
        assert result["passed"] is True
