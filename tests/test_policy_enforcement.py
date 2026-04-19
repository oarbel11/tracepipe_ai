import pytest
from tracepipe.governance.governance_policy import (
    GovernancePolicy,
    PolicySeverity,
)
from tracepipe.governance.policy_enforcement import (
    PolicyViolationDetector,
    EnforcementEngine,
)
from tracepipe.governance.compliance_dashboard import ComplianceDashboard
from tracepipe.lineage.interactive_lineage import InteractiveLineageGraph


def test_policy_violation_detection():
    policies = [
        GovernancePolicy(
            policy_id="P1",
            name="Policy 1",
            description="PII policy",
            tags=["pii"],
            severity=PolicySeverity.HIGH,
        )
    ]
    detector = PolicyViolationDetector(policies)
    violations = detector.check_asset("A1", "Asset 1", ["pii"])
    assert len(violations) == 1
    assert violations[0].policy.policy_id == "P1"


def test_high_risk_asset_detection():
    policies = [
        GovernancePolicy(
            policy_id="P2",
            name="Critical Policy",
            description="Critical data",
            tags=["critical"],
            severity=PolicySeverity.CRITICAL,
        )
    ]
    detector = PolicyViolationDetector(policies)
    engine = EnforcementEngine()
    violations = detector.check_asset("A2", "Asset 2", ["critical"])
    engine.process_violations(violations)
    high_risk = engine.get_high_risk_assets()
    assert "A2" in high_risk


def test_enforcement_engine_alerts():
    policies = [
        GovernancePolicy(
            policy_id="P3",
            name="Alert Policy",
            description="Alert test",
            tags=["sensitive"],
        )
    ]
    detector = PolicyViolationDetector(policies)
    engine = EnforcementEngine()
    violations = detector.check_asset("A3", "Asset 3", ["sensitive"])
    engine.process_violations(violations)
    assert len(engine.alerts) == 1


def test_remediation_suggestions():
    policies = [
        GovernancePolicy(
            policy_id="P4",
            name="Auto Policy",
            description="Auto remediate",
            tags=["auto"],
            auto_remediate=True,
        )
    ]
    detector = PolicyViolationDetector(policies)
    engine = EnforcementEngine()
    violations = detector.check_asset("A4", "Asset 4", ["auto"])
    engine.process_violations(violations)
    assert len(engine.actions) == 1


def test_interactive_lineage_enforcement():
    graph = InteractiveLineageGraph()
    graph.add_node("N1", "Node 1", ["pii"])
    policies = [
        GovernancePolicy(
            policy_id="P5",
            name="Lineage Policy",
            description="Test",
            tags=["pii"],
        )
    ]
    graph.attach_policy_enforcement(policies)
    graph.scan_for_violations()
    violations = graph.get_violations_for_node("N1")
    assert len(violations) == 1


def test_visualization_highlights():
    graph = InteractiveLineageGraph()
    graph.add_node("N2", "Node 2", ["critical"])
    policies = [
        GovernancePolicy(
            policy_id="P6",
            name="Viz Policy",
            description="Visual",
            tags=["critical"],
            severity=PolicySeverity.HIGH,
        )
    ]
    graph.attach_policy_enforcement(policies)
    graph.scan_for_violations()
    viz = graph.get_node_visualization_data("N2")
    assert viz["highlight"] == "high_risk"


def test_compliance_dashboard():
    engine = EnforcementEngine()
    policies = [
        GovernancePolicy(
            policy_id="P7",
            name="Dashboard Policy",
            description="Test",
            tags=["data"],
            severity=PolicySeverity.CRITICAL,
        )
    ]
    detector = PolicyViolationDetector(policies)
    violations = detector.check_asset("A7", "Asset 7", ["data"])
    engine.process_violations(violations)
    dashboard = ComplianceDashboard(engine)
    summary = dashboard.get_summary()
    assert summary["total_violations"] == 1
    assert summary["violations_by_severity"]["critical"] == 1
