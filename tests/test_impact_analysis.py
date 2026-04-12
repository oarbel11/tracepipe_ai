import pytest
from src.impact_analysis import InteractiveImpactAnalyzer


def test_interactive_analyzer_init():
    analyzer = InteractiveImpactAnalyzer()
    assert analyzer.config is not None


def test_analyze_changes():
    analyzer = InteractiveImpactAnalyzer()
    result = analyzer.analyze_changes({'files': ['file1.py', 'file2.py']})
    assert 'impact_score' in result
    assert 'risk_level' in result


def test_get_report():
    analyzer = InteractiveImpactAnalyzer()
    analyzer.analyze_changes({'files': ['file1.py']})
    report = analyzer.get_report()
    assert 'status' in report
    assert report['status'] == 'success'
    assert 'report' in report
