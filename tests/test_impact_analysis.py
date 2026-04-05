"""Tests for Interactive Impact Analysis."""
import pytest
from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer


def test_add_asset():
    """Test adding assets."""
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"tags": ["PII"], "owner": "team_a"})
    assert "table1" in analyzer.metadata
    assert analyzer.metadata["table1"]["tags"] == ["PII"]


def test_add_dependency():
    """Test adding dependencies."""
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {})
    analyzer.add_asset("table2", {})
    analyzer.add_dependency("table1", "table2")
    assert analyzer.graph.has_edge("table1", "table2")


def test_analyze_impact_basic():
    """Test basic impact analysis."""
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {})
    analyzer.add_asset("table2", {})
    analyzer.add_asset("table3", {})
    analyzer.add_dependency("table1", "table2")
    analyzer.add_dependency("table2", "table3")

    result = analyzer.analyze_impact("table1")
    assert result["count"] == 2
    assert "table2" in result["downstream"]
    assert "table3" in result["downstream"]


def test_analyze_impact_with_filters():
    """Test impact analysis with filters."""
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"tags": ["PII"]})
    analyzer.add_asset("table2", {"tags": ["PII"]})
    analyzer.add_asset("table3", {"tags": ["public"]})
    analyzer.add_dependency("table1", "table2")
    analyzer.add_dependency("table1", "table3")

    result = analyzer.analyze_impact("table1", {"tags": ["PII"]})
    assert result["count"] == 1
    assert "table2" in result["downstream"]
    assert "table3" not in result["downstream"]


def test_add_policy():
    """Test adding governance policies."""
    analyzer = InteractiveImpactAnalyzer()
    policy = {"name": "PII Policy", "target_tags": ["PII"]}
    analyzer.add_policy("policy1", policy)
    assert "policy1" in analyzer.policies


def test_policies_in_impact_analysis():
    """Test policies are included in impact analysis."""
    analyzer = InteractiveImpactAnalyzer()
    analyzer.add_asset("table1", {"tags": ["PII"]})
    analyzer.add_asset("table2", {"tags": ["PII"]})
    analyzer.add_dependency("table1", "table2")
    analyzer.add_policy("policy1", {"name": "PII", "target_tags": ["PII"]})

    result = analyzer.analyze_impact("table1")
    assert len(result["policies"]) > 0
    assert result["policies"][0]["name"] == "PII"


def test_nonexistent_asset():
    """Test analyzing nonexistent asset."""
    analyzer = InteractiveImpactAnalyzer()
    result = analyzer.analyze_impact("nonexistent")
    assert result["count"] == 0
    assert result["downstream"] == []
