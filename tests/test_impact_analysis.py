"""Tests for Interactive Impact Analysis."""

from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer


def test_add_asset():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"owner": "team_a", "tags": ["PII"]})
    assert "table1" in analyzer.graph


def test_add_dependency():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {})
    analyzer.add_asset("table2", {})
    analyzer.add_dependency("table1", "table2")
    assert analyzer.graph.has_edge("table1", "table2")


def test_analyze_impact_basic():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"owner": "team_a"})
    analyzer.add_asset("table2", {"owner": "team_b"})
    analyzer.add_dependency("table1", "table2")
    result = analyzer.analyze_impact("table1")
    assert result["source_asset"] == "table1"
    assert "table2" in result["impacted_assets"]
    assert result["total_count"] == 2


def test_analyze_impact_with_tag_filter():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"tags": ["PII"]})
    analyzer.add_asset("table2", {"tags": ["PII"]})
    analyzer.add_asset("table3", {"tags": ["public"]})
    analyzer.add_dependency("table1", "table2")
    analyzer.add_dependency("table1", "table3")
    result = analyzer.analyze_impact("table1", filters={"tags": ["PII"]})
    assert "table2" in result["impacted_assets"]
    assert "table3" not in result["impacted_assets"]


def test_analyze_impact_with_owner_filter():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"owner": "team_a"})
    analyzer.add_asset("table2", {"owner": "team_a"})
    analyzer.add_asset("table3", {"owner": "team_b"})
    analyzer.add_dependency("table1", "table2")
    analyzer.add_dependency("table1", "table3")
    result = analyzer.analyze_impact("table1", filters={"owner": "team_a"})
    assert "table2" in result["impacted_assets"]
    assert "table3" not in result["impacted_assets"]


def test_governance_policy_overlay():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {})
    analyzer.add_asset("table2", {})
    analyzer.add_dependency("table1", "table2")
    analyzer.add_governance_policy(
        "policy1",
        {"name": "PII Policy", "target_assets": ["table2"]}
    )
    result = analyzer.analyze_impact("table1")
    assert len(result["governance_policies"]) == 1
    assert result["governance_policies"][0]["name"] == "PII Policy"


def test_analyze_nonexistent_asset():
    analyzer = InteractiveImpactAnalyzer()
    result = analyzer.analyze_impact("nonexistent")
    assert "error" in result
