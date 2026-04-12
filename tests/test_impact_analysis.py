"""Tests for impact analysis."""
import pytest
from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer


def test_interactive_analyzer_init():
    """Test InteractiveImpactAnalyzer initialization."""
    analyzer = InteractiveImpactAnalyzer()
    assert analyzer is not None
    assert analyzer.config == {}


def test_analyze_changes():
    """Test analyzing changes."""
    analyzer = InteractiveImpactAnalyzer()
    result = analyzer.analyze_changes({
        "files": ["pipeline.py"],
        "type": "modification"
    })
    assert result["status"] == "success"
    assert "impact" in result


def test_get_report():
    """Test getting impact report."""
    analyzer = InteractiveImpactAnalyzer()
    report = analyzer.get_report()
    assert isinstance(report, str)
    assert "report" in report
