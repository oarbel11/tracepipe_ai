import pytest
import os
import tempfile
import yaml
from scripts.peer_review.workflow_engine import WorkflowEngine, PolicyEnforcer, NotificationService, AuditTrail
from scripts.peer_review.peer_review import PeerReviewOrchestrator

@pytest.fixture
def temp_config():
    config = {
        "policies": [
            {"id": "pii", "rule_type": "pii_check", "severity": "high", "message": "PII detected"},
            {"id": "downstream", "rule_type": "downstream_limit", "max_downstream": 3, "message": "Too many downstream"}
        ],
        "notifications": {"slack": {"enabled": True}}
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config, f)
        return f.name

def test_policy_enforcer():
    policies = [{"id": "p1", "rule_type": "pii_check", "message": "PII violation"}]
    enforcer = PolicyEnforcer(policies)
    result = enforcer.enforce({"pii_detected": True, "pii_approval": False})
    assert result["blocked"] is True
    assert len(result["violations"]) == 1

def test_notification_service():
    service = NotificationService({"slack": {"enabled": True}})
    notification = service.send(["user@test.com"], "Test message", "slack")
    assert notification["status"] == "sent"
    assert len(service.sent_notifications) == 1

def test_audit_trail():
    audit = AuditTrail()
    audit.log("PR-1", "started", "user", {"test": "data"})
    audit.log("PR-1", "approved", "admin", {"result": "pass"})
    trail = audit.get_trail("PR-1")
    assert len(trail) == 2
    assert trail[0]["action"] == "started"

def test_workflow_engine(temp_config):
    engine = WorkflowEngine(temp_config)
    result = engine.execute_workflow("PR-123", {"pii_detected": False, "downstream_count": 2}, "author@test.com")
    assert result["approved"] is True
    trail = engine.get_audit_trail("PR-123")
    assert len(trail) > 0

def test_workflow_blocks_violations(temp_config):
    engine = WorkflowEngine(temp_config)
    result = engine.execute_workflow("PR-124", {"pii_detected": True, "pii_approval": False}, "author@test.com")
    assert result["approved"] is False
    assert "violations" in result["reason"]

def test_peer_review_orchestrator(temp_config):
    orchestrator = PeerReviewOrchestrator(temp_config)
    result = orchestrator.review_change(
        "PR-125",
        {"columns": ["name", "email"], "downstream_count": 2},
        "dev@test.com"
    )
    assert "impact_analysis" in result
    assert result["impact_analysis"]["pii_detected"] is True

def test_get_review_status(temp_config):
    orchestrator = PeerReviewOrchestrator(temp_config)
    orchestrator.review_change("PR-126", {"downstream_count": 1}, "dev@test.com")
    status = orchestrator.get_review_status("PR-126")
    assert status["status"] in ["workflow_started", "approval_requested"]
