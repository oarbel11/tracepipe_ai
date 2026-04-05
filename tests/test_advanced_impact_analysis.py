import pytest
from src.blast_radius import BlastRadiusAnalyzer
from src.data_quality_integration import DataQualityMetrics
from src.advanced_impact_analyzer import AdvancedImpactAnalyzer


def test_data_quality_metrics():
    metrics = DataQualityMetrics()
    metrics.add_metric("table1", "null_count", 5.0, 10.0)
    assert len(metrics.get_metrics("table1")) == 1
    assert metrics.get_quality_score("table1") == 100.0


def test_quality_alerts():
    metrics = DataQualityMetrics()
    metrics.add_metric("table1", "error_rate", 15.0, 10.0)
    alerts = metrics.get_alerts("table1")
    assert len(alerts) == 1
    assert alerts[0]["severity"] in ["high", "medium"]


def test_advanced_impact_analyzer():
    analyzer = AdvancedImpactAnalyzer()
    analyzer.add_dependency("table1", "table2")
    analyzer.add_dependency("table2", "table3")
    downstream = analyzer.get_downstream_impact("table1")
    assert "table2" in downstream
    assert "table3" in downstream


def test_what_if_analysis():
    analyzer = AdvancedImpactAnalyzer()
    analyzer.add_dependency("source", "target1")
    analyzer.add_dependency("source", "target2")
    result = analyzer.analyze_what_if("source")
    assert result["affected_count"] == 2
    assert result["risk_level"] in ["low", "medium", "high"]


def test_root_cause_analysis():
    analyzer = AdvancedImpactAnalyzer()
    analyzer.add_dependency("source1", "target")
    analyzer.add_dependency("source2", "target")
    upstream = analyzer.get_root_cause_analysis("target")
    assert "source1" in upstream
    assert "source2" in upstream


def test_blast_radius_with_quality():
    blast = BlastRadiusAnalyzer()
    blast.add_lineage("table1", "table2")
    blast.add_quality_metric("table1", "null_count", 5.0, 10.0)
    result = blast.analyze_blast_radius("table1")
    assert "downstream_impact" in result
    assert "quality_score" in result
    assert result["quality_score"] == 100.0


def test_lineage_with_quality_enrichment():
    blast = BlastRadiusAnalyzer()
    blast.add_lineage("source", "target")
    blast.add_quality_metric("target", "error_rate", 2.0, 5.0)
    result = blast.get_lineage_with_quality("source")
    assert len(result["downstream"]) == 1
    assert result["downstream"][0]["table"] == "target"
    assert "quality_score" in result["downstream"][0]
