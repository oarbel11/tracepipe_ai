"""Tests for Git CI/CD integration."""
import pytest
from scripts.peer_review.git_integration import GitCICDIntegration


def test_git_integration_init():
    """Test Git integration initialization."""
    integration = GitCICDIntegration()
    assert integration is not None
    assert integration.analyzer is not None
    assert integration.enforcer is not None


def test_process_commit_low_risk():
    """Test processing a low-risk commit."""
    integration = GitCICDIntegration()
    result = integration.process_commit({
        "commit_id": "abc123",
        "files": ["pipeline.py"]
    })
    assert result["status"] == "success"


def test_webhook_handler_push():
    """Test webhook handler for push events."""
    integration = GitCICDIntegration()
    result = integration.webhook_handler("push", {
        "commit_id": "xyz789"
    })
    assert "status" in result


def test_webhook_handler_ignored():
    """Test webhook handler for ignored events."""
    integration = GitCICDIntegration()
    result = integration.webhook_handler("unknown", {})
    assert result["status"] == "ignored"
