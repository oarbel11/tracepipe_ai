import pytest
import json
from scripts.ci_cd.webhook_handler import CICDWebhookHandler
from scripts.ci_cd.policy_engine import PolicyEngine
from scripts.ci_cd.feedback_generator import FeedbackGenerator

@pytest.fixture
def webhook_handler():
    return CICDWebhookHandler(config_path='config/policies.yml')

@pytest.fixture
def policy_engine():
    return PolicyEngine(config_path='config/policies.yml')

@pytest.fixture
def feedback_generator():
    return FeedbackGenerator()

def test_policy_engine_pii_detection(policy_engine):
    pr_data = {
        'id': 123,
        'title': 'Add SSN column',
        'files': ['schema.sql'],
        'author': 'dev'
    }
    violations = policy_engine.evaluate(pr_data)
    assert len(violations) > 0
    assert any(v['rule_type'] == 'pii_detection' for v in violations)

def test_policy_engine_schema_breaking(policy_engine):
    pr_data = {
        'id': 124,
        'title': 'Drop user column',
        'files': ['migration.sql'],
        'author': 'dev'
    }
    violations = policy_engine.evaluate(pr_data)
    assert any(v['rule_type'] == 'schema_breaking' for v in violations)

def test_policy_engine_no_violations(policy_engine):
    pr_data = {
        'id': 125,
        'title': 'Update documentation',
        'files': ['README.md'],
        'author': 'dev'
    }
    violations = policy_engine.evaluate(pr_data)
    schema_violations = [v for v in violations if v['rule_type'] == 'schema_breaking']
    assert len(schema_violations) == 0

def test_feedback_generator_critical_violation(feedback_generator):
    violations = [{
        'policy_id': 'pii-block',
        'name': 'PII Detection',
        'severity': 'critical',
        'message': 'PII detected',
        'rule_type': 'pii_detection'
    }]
    feedback = feedback_generator.generate(violations, {})
    assert 'critical' in feedback['summary'].lower()
    assert feedback['can_merge'] is False
    assert len(feedback['suggestions']) > 0

def test_feedback_generator_no_violations(feedback_generator):
    feedback = feedback_generator.generate([], {})
    assert 'passed' in feedback['summary'].lower()
    assert len(feedback['suggestions']) == 0

def test_webhook_handler_github_pr(webhook_handler):
    event = {
        'action': 'opened',
        'pull_request': {
            'number': 42,
            'title': 'Add credit card field',
            'changed_files': ['user_schema.sql'],
            'user': {'login': 'testuser'}
        }
    }
    result = webhook_handler.process_event(event)
    assert 'status' in result
    assert 'violations' in result
    assert 'feedback' in result

def test_webhook_handler_gitlab_mr(webhook_handler):
    event = {
        'object_kind': 'merge_request',
        'object_attributes': {
            'iid': 43,
            'title': 'Update README',
            'author': {'username': 'testuser'}
        }
    }
    result = webhook_handler.process_event(event)
    assert result['status'] in ['success', 'failure']
