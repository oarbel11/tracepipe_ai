import pytest
import time
from scripts.impact_analysis import ImpactAnalyzer, Alert
from scripts.anomaly_detector import AnomalyDetector
from scripts.impact_dashboard import ImpactDashboard


def test_basic_lineage_tracking():
    analyzer = ImpactAnalyzer()
    analyzer.track_lineage("table_a", [])
    analyzer.track_lineage("table_b", ["table_a"])
    analyzer.track_lineage("table_c", ["table_b"])
    
    impact = analyzer.get_downstream_impact("table_a")
    assert "table_b" in impact["affected_assets"]
    assert "table_c" in impact["affected_assets"]
    assert impact["depth"] == 2


def test_rename_preserves_lineage():
    analyzer = ImpactAnalyzer()
    analyzer.track_lineage("old_table", [])
    analyzer.track_lineage("downstream_table", ["old_table"])
    
    analyzer.handle_rename("old_table", "new_table")
    
    impact = analyzer.get_downstream_impact("new_table")
    assert "downstream_table" in impact["affected_assets"]
    
    asset_id = analyzer.name_mapping["new_table"]
    asset = analyzer.assets[asset_id]
    assert asset.current_name == "new_table"
    assert "old_table" in asset.previous_names


def test_versioned_lineage():
    analyzer = ImpactAnalyzer()
    analyzer.track_lineage("table_x", ["source_1"])
    time.sleep(0.01)
    analyzer.track_lineage("table_x", ["source_1", "source_2"])
    
    asset_id = analyzer.name_mapping["table_x"]
    asset = analyzer.assets[asset_id]
    assert len(asset.versions) == 2
    assert asset.versions[0].version == 1
    assert len(asset.versions[1].upstream) == 2


def test_alert_creation_with_impact():
    analyzer = ImpactAnalyzer()
    analyzer.track_lineage("source", [])
    analyzer.track_lineage("model", ["source"])
    analyzer.track_lineage("dashboard", ["model"])
    
    alert = analyzer.create_alert("source", "schema_change",
                                  "Column removed", "high")
    assert alert.severity == "high"
    assert "model" in alert.affected_downstream
    assert "dashboard" in alert.affected_downstream


def test_schema_drift_detection():
    detector = AnomalyDetector()
    detector.capture_schema("table_y", {"id": "int", "name": "string"})
    time.sleep(0.01)
    detector.capture_schema("table_y", {"id": "int", "email": "string"})
    
    drift = detector.detect_schema_drift("table_y")
    assert drift is not None
    assert "name" in drift["removed_columns"]
    assert "email" in drift["added_columns"]


def test_data_quality_anomaly():
    analyzer = ImpactAnalyzer()
    detector = AnomalyDetector(analyzer)
    detector.capture_schema("table_z", {"col1": "int"}, row_count=1000)
    
    anomaly = detector.detect_data_quality_anomaly(
        "table_z", {"null_rate": 0.25, "row_count": 200}
    )
    assert anomaly is not None
    assert len(anomaly["anomalies"]) > 0


def test_dashboard_integration():
    dashboard = ImpactDashboard()
    dashboard.analyzer.track_lineage("raw.users", [])
    dashboard.analyzer.track_lineage("staging.users", ["raw.users"])
    dashboard.analyzer.handle_rename("raw.users", "raw.users_v2")
    
    lineage = dashboard.get_lineage_graph("raw.users_v2")
    assert "staging.users" in lineage["downstream"]
    assert "raw.users" in lineage["previous_names"]
    
    summary = dashboard.get_dashboard_summary()
    assert summary["total_assets"] >= 2
    assert summary["total_lineage_edges"] >= 1


def test_resolve_renamed_asset():
    dashboard = ImpactDashboard()
    dashboard.analyzer.track_lineage("old_name", [])
    dashboard.analyzer.handle_rename("old_name", "new_name")
    
    resolved = dashboard.resolve_asset_name("old_name")
    assert resolved == "new_name"
