"""Tests for impact analysis and governance policy features."""
import pytest
from scripts.peer_review.impact_analysis import ImpactAnalysisEngine, ImpactNode
from scripts.peer_review.governance_policy import (
    GovernancePolicyEngine,
    GovernancePolicy,
    PolicyViolation
)


@pytest.fixture
def sample_lineage_graph():
    """Sample lineage graph for testing."""
    return {
        "nodes": [
            {"id": "table1", "type": "table", "metadata": {"tags": ["PII"], "owner": "team_a"}},
            {"id": "table2", "type": "table", "metadata": {"tags": ["analytics"], "owner": "team_b"}},
            {"id": "view1", "type": "view", "metadata": {"tags": ["PII"], "quality_status": "good"}}
        ],
        "edges": [
            {"source": "table1", "target": "view1"},
            {"source": "view1", "target": "table2"}
        ]
    }


def test_impact_analysis_downstream(sample_lineage_graph):
    """Test downstream impact analysis."""
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    impact = engine.compute_downstream_impact("table1")
    assert len(impact) == 3
    assert impact[0].asset_id == "table1"
    assert impact[0].depth == 0


def test_impact_analysis_with_tag_filter(sample_lineage_graph):
    """Test impact analysis with tag filter."""
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    impact = engine.compute_downstream_impact("table1", filters={"tags": ["PII"]})
    pii_assets = [node for node in impact if "PII" in node.metadata.get("tags", [])]
    assert len(pii_assets) == 2


def test_impact_analysis_with_owner_filter(sample_lineage_graph):
    """Test impact analysis with owner filter."""
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    impact = engine.compute_downstream_impact("table1", filters={"owner": "team_a"})
    assert len(impact) == 1
    assert impact[0].metadata["owner"] == "team_a"


def test_governance_policy_evaluation(sample_lineage_graph):
    """Test governance policy evaluation."""
    policy = GovernancePolicy(
        policy_id="pol1",
        name="PII Tagging Required",
        description="All tables must have PII tag",
        rules={"required_tags": ["PII"]},
        severity="high"
    )
    engine = GovernancePolicyEngine([policy])
    violations = engine.evaluate_lineage(sample_lineage_graph)
    assert len(violations) >= 1


def test_governance_policy_no_violations():
    """Test governance policy with no violations."""
    graph = {
        "nodes": [{"id": "t1", "type": "table", "metadata": {"tags": ["PII"]}}],
        "edges": []
    }
    policy = GovernancePolicy(
        policy_id="pol1",
        name="PII Required",
        description="Test",
        rules={"required_tags": ["PII"]},
        severity="high"
    )
    engine = GovernancePolicyEngine([policy])
    violations = engine.evaluate_lineage(graph)
    assert len(violations) == 0
