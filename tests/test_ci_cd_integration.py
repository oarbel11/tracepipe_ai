"""Tests for CI/CD integration."""
import pytest
import json
from scripts.ci_cd.webhook_handler import CICDWebhookHandler
from scripts.ci_cd.policy_engine import PolicyEngine
from scripts.ci_cd.feedback_generator import FeedbackGenerator
from scripts.ci_cd.ci_cd_plugin import CICDPlugin


class TestPolicyEngine:
    def test_default_policies(self):
        engine = PolicyEngine()
        assert 'pii_detection' in engine.policies
        assert 'schema_changes' in engine.policies

    def test_pii_detection(self):
        engine = PolicyEngine()
        pr_data = {'diff': 'add ssn field', 'files': []}
        violations = engine.evaluate(pr_data)
        assert len(violations) > 0
        assert violations[0]['type'] == 'pii_exposure'

    def test_schema_check(self):
        engine = PolicyEngine()
        pr_data = {'diff': 'DROP TABLE users', 'files': ['schema.sql']}
        violations = engine.evaluate(pr_data)
        assert any(v['type'] == 'schema_breaking' for v in violations)

    def test_performance_check(self):
        engine = PolicyEngine()
        pr_data = {'diff': 'SELECT * FROM table', 'files': ['query.sql']}
        violations = engine.evaluate(pr_data)
        assert any(v['type'] == 'performance_issue' for v in violations)


class TestWebhookHandler:
    def test_github_webhook(self):
        handler = CICDWebhookHandler()
        payload = {
            'action': 'opened',
            'pull_request': {'number': 123, 'title': 'Test PR', 'body': 'test'},
            'files': []
        }
        result = handler.handle_github_webhook(payload)
        assert 'status' in result
        assert 'pr_id' in result

    def test_gitlab_webhook(self):
        handler = CICDWebhookHandler()
        payload = {
            'object_kind': 'merge_request',
            'object_attributes': {'iid': 456, 'title': 'Test MR', 'description': 'test'}
        }
        result = handler.handle_gitlab_webhook(payload)
        assert 'status' in result
        assert 'pr_id' in result


class TestFeedbackGenerator:
    def test_no_violations(self):
        gen = FeedbackGenerator()
        feedback = gen.generate([])
        assert 'passed' in feedback.lower()

    def test_critical_violation(self):
        gen = FeedbackGenerator()
        violations = [{'type': 'pii_exposure', 'severity': 'critical', 'message': 'PII found'}]
        feedback = gen.generate(violations)
        assert 'Critical' in feedback


class TestCICDPlugin:
    def test_process_github_webhook(self):
        plugin = CICDPlugin()
        payload = {'action': 'opened', 'pull_request': {'number': 1, 'title': 'Test', 'body': ''}, 'files': []}
        result = plugin.process_webhook('github', payload)
        assert result['status'] in ['success', 'failure']

    def test_unsupported_platform(self):
        plugin = CICDPlugin()
        result = plugin.process_webhook('unknown', {})
        assert result['status'] == 'error'
