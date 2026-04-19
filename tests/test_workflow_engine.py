import pytest
import json
from tracepipe_ai import WorkflowEngine, PolicyEnforcer, NotificationService, AuditTrail, PeerReviewSystem, ApprovalStatus

@pytest.fixture
def workflow_config():
    return {
        "policies": {
            "max_rows_affected": 5000,
            "blocked_operations": ["DROP"]
        },
        "approvals": {
            "critical": ["senior_eng", "dba"],
            "high": ["team_lead"],
            "medium": ["peer"],
            "low": []
        },
        "notifications": {
            "enabled": True
        }
    }

@pytest.fixture
def workflow_engine(workflow_config):
    return WorkflowEngine(workflow_config)

def test_policy_enforcer_compliant():
    enforcer = PolicyEnforcer({"max_rows_affected": 1000})
    result = enforcer.evaluate({"rows_affected": 500})
    assert result["compliant"] is True
    assert len(result["violations"]) == 0

def test_policy_enforcer_violation():
    enforcer = PolicyEnforcer({"max_rows_affected": 1000})
    result = enforcer.evaluate({"rows_affected": 1500})
    assert result["compliant"] is False
    assert len(result["violations"]) > 0

def test_policy_enforcer_blocked_operation():
    enforcer = PolicyEnforcer({"blocked_operations": ["DROP"]})
    result = enforcer.evaluate({"operation": "DROP"})
    assert result["compliant"] is False

def test_workflow_engine_blocks_noncompliant(workflow_engine):
    change_data = {"id": "1", "operation": "UPDATE"}
    impact_analysis = {"severity": "high", "rows_affected": 10000}
    result = workflow_engine.process_change(change_data, impact_analysis)
    assert result["status"] == ApprovalStatus.BLOCKED.value

def test_workflow_engine_routes_approval(workflow_engine):
    change_data = {"id": "2", "operation": "UPDATE"}
    impact_analysis = {"severity": "high", "rows_affected": 2000}
    result = workflow_engine.process_change(change_data, impact_analysis)
    assert result["status"] == ApprovalStatus.PENDING.value
    assert "team_lead" in result["approvers"]

def test_notification_service():
    service = NotificationService({"enabled": True})
    service.send_alert("Test Alert", {"detail": "test"})
    assert len(service.sent_notifications) == 1
    assert service.sent_notifications[0]["type"] == "alert"

def test_audit_trail():
    trail = AuditTrail()
    trail.log("APPROVED", {"id": "1"}, {"approver": "user1"})
    logs = trail.get_logs()
    assert len(logs) == 1
    assert logs[0]["action"] == "APPROVED"

def test_peer_review_integration(workflow_config):
    system = PeerReviewSystem(workflow_config)
    change = {"operation": "UPDATE", "rows_affected": 200}
    result = system.review_change(change)
    assert "impact_analysis" in result
    assert "workflow_result" in result
