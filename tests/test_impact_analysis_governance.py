import pytest
import networkx as nx
from scripts.peer_review.impact_analysis import ImpactAnalysisEngine
from scripts.peer_review.governance_policy import GovernancePolicyEngine


@pytest.fixture
def sample_lineage_graph():
    G = nx.DiGraph()
    G.add_node("table_a", type="table", tags=["PII"], owner="alice",
               quality_status="good", encrypted=True)
    G.add_node("table_b", type="table", tags=["analytics"], owner="bob",
               quality_status="critical", encrypted=False)
    G.add_node("table_c", type="view", tags=["PII", "reporting"],
               owner="unassigned", quality_status="good", encrypted=False)
    G.add_node("table_d", type="table", tags=["archive"], owner="alice",
               quality_status="good", encrypted=True)
    G.add_edge("table_a", "table_b")
    G.add_edge("table_a", "table_c")
    G.add_edge("table_b", "table_d")
    return G


def test_impact_analysis_basic(sample_lineage_graph):
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    result = engine.analyze_impact("table_a")
    assert result["source_asset"] == "table_a"
    assert result["total_impacted"] == 3
    assert len(result["impacted_assets"]) == 3


def test_impact_analysis_with_tag_filter(sample_lineage_graph):
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    result = engine.analyze_impact("table_a", filters={"tags": ["PII"]})
    assert result["total_impacted"] == 1
    assert result["impacted_assets"][0]["id"] == "table_c"


def test_impact_analysis_with_owner_filter(sample_lineage_graph):
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    result = engine.analyze_impact("table_a", filters={"owner": "alice"})
    assert result["total_impacted"] == 1
    assert result["impacted_assets"][0]["id"] == "table_d"


def test_impact_analysis_critical_paths(sample_lineage_graph):
    engine = ImpactAnalysisEngine(sample_lineage_graph)
    result = engine.analyze_impact("table_a")
    assert len(result["critical_paths"]) >= 1


def test_governance_policy_registration(sample_lineage_graph):
    engine = GovernancePolicyEngine(sample_lineage_graph)
    engine.register_policy(
        "pol_001",
        "data_privacy",
        {"encryption_required": True, "owner_required": True}
    )
    assert "pol_001" in engine.policies


def test_governance_policy_overlay(sample_lineage_graph):
    engine = GovernancePolicyEngine(sample_lineage_graph)
    engine.register_policy(
        "pol_001",
        "data_privacy",
        {"encryption_required": True, "owner_required": True}
    )
    overlay = engine.overlay_policies(["table_a", "table_c"])
    assert "table_a" in overlay
    assert "table_c" in overlay
    assert len(overlay["table_a"]["policies"]) > 0


def test_governance_violations(sample_lineage_graph):
    engine = GovernancePolicyEngine(sample_lineage_graph)
    engine.register_policy(
        "pol_001",
        "data_privacy",
        {"encryption_required": True, "owner_required": True}
    )
    overlay = engine.overlay_policies(["table_c"])
    assert len(overlay["table_c"]["violations"]) >= 1
    assert overlay["table_c"]["compliance_score"] < 100.0


def test_compliance_score_calculation(sample_lineage_graph):
    engine = GovernancePolicyEngine(sample_lineage_graph)
    engine.register_policy(
        "pol_001",
        "data_privacy",
        {"encryption_required": True}
    )
    overlay = engine.overlay_policies(["table_a"])
    assert overlay["table_a"]["compliance_score"] == 100.0
