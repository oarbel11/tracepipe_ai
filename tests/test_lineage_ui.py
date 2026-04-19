import pytest
from unittest.mock import Mock
from scripts.lineage_ui_manager import LineageUIManager

@pytest.fixture
def manager():
    return LineageUIManager()

@pytest.fixture
def populated_manager():
    mgr = LineageUIManager()
    mgr.add_lineage_node("table1", "table", {"name": "sales"})
    mgr.add_lineage_node("table2", "table", {"name": "orders"})
    mgr.add_lineage_node("table3", "table", {"name": "customers"})
    mgr.add_lineage_edge("table1", "table2", "join")
    mgr.add_lineage_edge("table2", "table3", "aggregate")
    return mgr

def test_add_lineage_node(manager):
    manager.add_lineage_node("test_table", "table", {"name": "test"})
    assert "test_table" in manager.lineage_graph.nodes()
    assert manager.lineage_graph.nodes["test_table"]["type"] == "table"

def test_add_lineage_edge(manager):
    manager.add_lineage_node("source", "table", {"name": "src"})
    manager.add_lineage_node("target", "table", {"name": "tgt"})
    manager.add_lineage_edge("source", "target", "transform")
    assert manager.lineage_graph.has_edge("source", "target")

def test_get_lineage_graph(populated_manager):
    result = populated_manager.get_lineage_graph("table1")
    assert len(result["nodes"]) > 0
    assert len(result["edges"]) > 0

def test_get_lineage_graph_missing_node(manager):
    result = manager.get_lineage_graph("nonexistent")
    assert result["nodes"] == []
    assert result["edges"] == []

def test_impact_analysis(populated_manager):
    result = populated_manager.impact_analysis("table1", "schema_change")
    assert "impacted_nodes" in result
    assert "risk_level" in result
    assert result["change_type"] == "schema_change"

def test_impact_analysis_missing_node(manager):
    result = manager.impact_analysis("nonexistent", "test")
    assert result["risk_level"] == "none"

def test_add_classification(manager):
    manager.add_lineage_node("table1", "table", {"name": "test"})
    manager.add_classification("table1", "PII")
    assert manager.classifications["table1"] == "PII"

def test_add_glossary_term(manager):
    manager.add_glossary_term("table1", "Customer", "A person who purchases")
    assert manager.glossary_terms["table1"]["term"] == "Customer"

def test_add_masking_policy(manager):
    manager.add_masking_policy("table1", "mask_email")
    assert manager.masking_policies["table1"] == "mask_email"

def test_get_governance_info(manager):
    manager.add_classification("table1", "PII")
    manager.add_glossary_term("table1", "User", "App user")
    manager.add_masking_policy("table1", "hash")
    info = manager.get_governance_info("table1")
    assert info["classification"] == "PII"
    assert info["glossary_term"]["term"] == "User"
    assert info["masking_policy"] == "hash"

def test_detect_lineage_issues(manager):
    manager.add_lineage_node("isolated", "table", {"name": "test"})
    issues = manager.detect_lineage_issues()
    assert len(issues) == 1
    assert issues[0]["issue"] == "isolated_node"

def test_export_lineage(populated_manager):
    result = populated_manager.export_lineage()
    assert "nodes" in result
    assert "links" in result
