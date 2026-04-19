import pytest
import sys
from scripts.lineage_ui_manager import LineageUIManager

def test_add_lineage():
    manager = LineageUIManager()
    manager.add_lineage("table_a", "table_b")
    assert "table_a" in manager.lineage_graph.nodes()
    assert "table_b" in manager.lineage_graph.nodes()

def test_get_upstream():
    manager = LineageUIManager()
    manager.add_lineage("table_a", "table_b")
    manager.add_lineage("table_b", "table_c")
    upstream = manager.get_upstream("table_c")
    assert "table_a" in upstream
    assert "table_b" in upstream

def test_get_downstream():
    manager = LineageUIManager()
    manager.add_lineage("table_a", "table_b")
    manager.add_lineage("table_b", "table_c")
    downstream = manager.get_downstream("table_a")
    assert "table_b" in downstream
    assert "table_c" in downstream

def test_analyze_impact():
    manager = LineageUIManager()
    manager.add_lineage("table_a", "table_b")
    manager.add_lineage("table_b", "table_c")
    impact = manager.analyze_impact("table_a", "schema_change")
    assert impact["impact_count"] == 2
    assert "table_b" in impact["affected_nodes"]

def test_add_governance():
    manager = LineageUIManager()
    manager.add_governance("table_a", "classification", "PII")
    gov = manager.get_governance("table_a")
    assert gov["classification"] == "PII"

def test_add_annotation():
    manager = LineageUIManager()
    manager.add_annotation("table_a", "This is a test")
    assert len(manager.annotations["table_a"]) == 1

def test_detect_issues():
    manager = LineageUIManager()
    manager.lineage_graph.add_node("isolated_table")
    issues = manager.detect_issues()
    assert len(issues) > 0
    assert issues[0]["type"] == "isolated_node"

def test_export_lineage():
    manager = LineageUIManager()
    manager.add_lineage("table_a", "table_b")
    export = manager.export_lineage()
    assert "table_a" in export
    assert "table_b" in export
