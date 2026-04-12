import pytest
from src.cicd_integration import GitCICDIntegration


def test_git_integration_init():
    integration = GitCICDIntegration()
    assert integration.analyzer is not None
    assert integration.enforcer is not None


def test_process_commit_low_risk():
    integration = GitCICDIntegration()
    result = integration.process_commit({'files': ['file1.py']})
    assert 'status' in result
    assert result['status'] in ['success', 'failed']


def test_webhook_handler_push():
    integration = GitCICDIntegration()
    result = integration.webhook_handler('push', {'files': ['file1.py']})
    assert 'status' in result


def test_webhook_handler_ignored():
    integration = GitCICDIntegration()
    result = integration.webhook_handler('unknown_event', {})
    assert result['status'] == 'ignored'
