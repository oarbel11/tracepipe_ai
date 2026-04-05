import pytest
from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer
from scripts.peer_review.governance import GovernancePolicy, PolicyEngine


def test_basic_impact_analysis():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table_a", {"tags": ["PII"], "owner": "team1"})
    analyzer.add_asset("table_b", {"tags": ["analytics"], "owner": "team2"})
    analyzer.add_asset("table_c", {"tags": ["PII"], "owner": "team1"})
    analyzer.add_dependency("table_a", "table_b")
    analyzer.add_dependency("table_a", "table_c")
    
    result = analyzer.analyze_impact("table_a")
    assert result.root_asset == "table_a"
    assert len(result.affected_assets) == 2


def test_impact_analysis_with_tag_filter():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table_a", {"tags": ["PII"]})
    analyzer.add_asset("table_b", {"tags": ["PII"]})
    analyzer.add_asset("table_c", {"tags": ["analytics"]})
    analyzer.add_dependency("table_a", "table_b")
    analyzer.add_dependency("table_a", "table_c")
    
    result = analyzer.analyze_impact("table_a", filters={"tags": ["PII"]})
    assert len(result.affected_assets) == 1
    assert result.affected_assets[0]["name"] == "table_b"


def test_impact_analysis_with_depth_limit():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table_a", {})
    analyzer.add_asset("table_b", {})
    analyzer.add_asset("table_c", {})
    analyzer.add_dependency("table_a", "table_b")
    analyzer.add_dependency("table_b", "table_c")
    
    result = analyzer.analyze_impact("table_a", max_depth=1)
    assert len(result.affected_assets) == 1
    assert result.affected_assets[0]["name"] == "table_b"


def test_governance_policy_application():
    policy = GovernancePolicy(name="PII Policy", tags=["PII"], rules=["Encrypt"])
    assert policy.applies_to_asset({"tags": ["PII", "customer"]})
    assert not policy.applies_to_asset({"tags": ["analytics"]})


def test_policy_overlay_on_impact():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table_a", {"tags": ["PII"]})
    analyzer.add_asset("table_b", {"tags": ["PII"]})
    analyzer.add_dependency("table_a", "table_b")
    
    policy = GovernancePolicy(name="Test Policy", tags=["PII"], rules=["Test Rule"])
    result = analyzer.analyze_impact("table_a", policies=[policy])
    
    assert len(result.affected_assets) == 1
    assert len(result.affected_assets[0]["policies"]) > 0
    assert any(p["name"] == "Test Policy" for p in result.affected_assets[0]["policies"])
