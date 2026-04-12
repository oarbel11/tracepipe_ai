"""Tests for CI/CD integration."""
import pytest
import json
from scripts.cicd.webhook_handler import WebhookHandler
from scripts.cicd.policy_enforcer import PolicyEnforcer
from scripts.cicd.workflow_generator import WorkflowGenerator
from scripts.peer_review.impact_analyzer import ImpactAnalyzer


class TestWebhookHandler:
    def test_handle_github_webhook(self):
        handler = WebhookHandler()
        payload = {
            "pull_request": {"changed_files": ["pipeline.sql"]},
            "repository": {"name": "test-repo"}
        }
        result = handler.handle_webhook(payload, "github")
        assert result["status"] == "success"
        assert result["event_type"] == "pull_request"


class TestPolicyEnforcer:
    def test_enforce_policies_no_violations(self):
        enforcer = PolicyEnforcer()
        changes = {"files": ["pipeline.sql"]}
        result = enforcer.enforce_policies(changes)
        assert "passed" in result
        assert "impact" in result
        assert "violations" in result

    def test_enforce_policies_with_high_severity(self):
        enforcer = PolicyEnforcer()
        changes = {"files": [f"pipeline{i}.sql" for i in range(10)]}
        result = enforcer.enforce_policies(changes)
        assert result["requires_approval"] is True


class TestImpactAnalyzer:
    def test_analyze_changes(self):
        analyzer = ImpactAnalyzer()
        changes = {"files": ["pipeline.sql", "transform.sql"]}
        result = analyzer.analyze_changes(changes)
        assert "impacted_pipelines" in result
        assert "severity" in result
        assert len(result["impacted_pipelines"]) == 2


class TestWorkflowGenerator:
    def test_generate_github_actions(self):
        generator = WorkflowGenerator()
        config = {"branches": ["main", "develop"]}
        workflow = generator.generate_github_actions(config)
        assert "name: Tracepipe Policy Check" in workflow
        assert "pull_request:" in workflow

    def test_generate_gitlab_ci(self):
        generator = WorkflowGenerator()
        config = {}
        workflow = generator.generate_gitlab_ci(config)
        assert "policy-check:" in workflow
        assert "merge_requests" in workflow
