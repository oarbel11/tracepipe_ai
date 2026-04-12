import pytest
from scripts.impact_analysis import ImpactAnalyzer, Alert, LineageVersion
from scripts.anomaly_detector import AnomalyDetector
from scripts.impact_dashboard import ImpactDashboard

def test_track_lineage():
    analyzer = ImpactAnalyzer()
    schema = {"col1": "string", "col2": "int"}
    analyzer.track_lineage("table_a", schema, [], ["table_b"])
    assert "table_a" in analyzer.lineage_versions
    assert len(analyzer.lineage_versions["table_a"]) == 1
    assert analyzer.lineage_versions["table_a"][0].version == 1

def test_handle_rename():
    analyzer = ImpactAnalyzer()
    schema = {"col1": "string"}
    analyzer.track_lineage("old_table", schema, [], [])
    analyzer.handle_rename("old_table", "new_table")
    assert "new_table" in analyzer.lineage_versions
    assert analyzer.lineage_versions["new_table"][0].asset_id == "new_table"
    assert analyzer.rename_map["old_table"] == "new_table"

def test_get_downstream_assets():
    analyzer = ImpactAnalyzer()
    analyzer.track_lineage("A", {}, [], ["B"])
    analyzer.track_lineage("B", {}, ["A"], ["C"])
    analyzer.track_lineage("C", {}, ["B"], [])
    downstream = analyzer.get_downstream_assets("A")
    assert "B" in downstream
    assert "C" in downstream

def test_detect_schema_change():
    analyzer = ImpactAnalyzer()
    schema_v1 = {"col1": "string"}
    schema_v2 = {"col1": "string", "col2": "int"}
    analyzer.track_lineage("table_x", schema_v1, [], ["table_y"])
    alert = analyzer.detect_schema_change("table_x", schema_v2)
    assert alert is not None
    assert alert.severity == "high"
    assert "table_y" in alert.affected_assets

def test_anomaly_detector():
    detector = AnomalyDetector()
    detector.set_baseline("table_a", {"row_count": 1000, "null_count": 5})
    anomaly = detector.detect_data_quality_anomaly("table_a", {"row_count": 500, "null_count": 5})
    assert anomaly is not None
    assert anomaly.anomaly_type == "data_quality"

def test_impact_dashboard_summary():
    dashboard = ImpactDashboard()
    dashboard.track_asset("A", {"col": "string"}, [], ["B", "C"])
    summary = dashboard.get_impact_summary("A")
    assert summary["downstream_count"] == 2
    assert "B" in summary["downstream_assets"]

def test_impact_dashboard_alerts():
    dashboard = ImpactDashboard()
    dashboard.track_asset("T1", {"a": "int"}, [], ["T2"])
    dashboard.check_schema_change("T1", {"a": "string"})
    alerts = dashboard.get_alerts()
    assert len(alerts) > 0
    assert alerts[0]["severity"] == "high"

def test_multiple_versions():
    analyzer = ImpactAnalyzer()
    analyzer.track_lineage("table", {"v": 1}, [], [])
    analyzer.track_lineage("table", {"v": 2}, [], [])
    assert len(analyzer.lineage_versions["table"]) == 2
    assert analyzer.lineage_versions["table"][1].version == 2

def test_no_schema_change_no_alert():
    analyzer = ImpactAnalyzer()
    schema = {"col": "int"}
    analyzer.track_lineage("stable", schema, [], [])
    alert = analyzer.detect_schema_change("stable", schema)
    assert alert is None
