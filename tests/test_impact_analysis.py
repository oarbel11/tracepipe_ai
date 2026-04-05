import json
from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer
from scripts.peer_review.governance_policy import GovernancePolicy

def test_basic_impact_analysis():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table_a", {"tags": ["pii"], "owner": "team1"})
    analyzer.add_asset("table_b", {"tags": ["analytics"], "owner": "team2"})
    analyzer.add_asset("table_c", {"tags": ["pii", "sensitive"], "owner": "team1"})
    
    analyzer.add_dependency("table_a", "table_b")
    analyzer.add_dependency("table_a", "table_c")
    
    result = analyzer.analyze_impact("table_a")
    assert result["source_asset"] == "table_a"
    assert result["total_impacted"] == 2
    assert len(result["impacted_assets"]) == 2

def test_filtered_impact_analysis():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("source", {"tags": [], "owner": "admin"})
    analyzer.add_asset("downstream1", {"tags": ["pii"], "owner": "team1"})
    analyzer.add_asset("downstream2", {"tags": ["public"], "owner": "team2"})
    
    analyzer.add_dependency("source", "downstream1")
    analyzer.add_dependency("source", "downstream2")
    
    result = analyzer.analyze_impact("source", {"tags": ["pii"]})
    assert result["total_impacted"] == 1
    assert result["impacted_assets"][0]["asset_id"] == "downstream1"

def test_governance_policy_overlay():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("data_table", {"tags": ["pii"], "owner": "compliance"})
    analyzer.add_asset("report", {"tags": ["pii", "customer"], "owner": "analytics"})
    analyzer.add_dependency("data_table", "report")
    
    policy = GovernancePolicy(
        policy_id="POL001",
        name="PII Protection",
        description="All PII data must be encrypted",
        tags=["pii"],
        rules={"encryption": "required"},
        severity="high"
    )
    analyzer.add_policy(policy)
    
    result = analyzer.analyze_impact("data_table")
    assert len(result["policies"]) == 1
    assert result["policies"][0]["policy_id"] == "POL001"
    assert len(result["impacted_assets"][0]["applicable_policies"]) == 1

def test_nonexistent_asset():
    analyzer = InteractiveImpactAnalyzer()
    result = analyzer.analyze_impact("nonexistent")
    assert "error" in result
    assert result["impacted_assets"] == []

def test_policy_matching():
    policy = GovernancePolicy(
        policy_id="P1",
        name="Test Policy",
        description="Test",
        tags=["sensitive"]
    )
    assert policy.matches_asset(["sensitive", "other"], "asset1")
    assert not policy.matches_asset(["public"], "asset1")
