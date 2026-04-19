import pytest
import os
from scripts.lineage_ui_manager import LineageUIManager
from scripts.governance_ui_api import GovernanceUIAPI
from scripts.impact_visualizer import ImpactVisualizer
from scripts.peer_review.governance_policy import GovernancePolicy

@pytest.fixture
def lineage_manager(tmp_path):
    db_path = tmp_path / "test_lineage.duckdb"
    return LineageUIManager(str(db_path))

@pytest.fixture
def governance_api(lineage_manager):
    return GovernanceUIAPI(lineage_manager)

@pytest.fixture
def impact_viz(lineage_manager):
    return ImpactVisualizer(lineage_manager)

def test_lineage_persistence(lineage_manager):
    lineage_manager.add_lineage_edge("source_table", "target_table", "SELECT * FROM source")
    edges = lineage_manager.conn.execute("SELECT * FROM lineage_edges").fetchall()
    assert len(edges) == 1
    assert edges[0][1] == "source_table"

def test_apply_classification(lineage_manager):
    lineage_manager.apply_classification("test_table", "PII", "Contains emails")
    metadata = lineage_manager.conn.execute(
        "SELECT classifications FROM asset_metadata WHERE asset_id = ?", ["test_table"]
    ).fetchone()
    assert metadata is not None
    assert "PII" in metadata[0]

def test_business_term(lineage_manager):
    lineage_manager.add_business_term("test_table", "Customer Data")
    metadata = lineage_manager.conn.execute(
        "SELECT business_terms FROM asset_metadata WHERE asset_id = ?", ["test_table"]
    ).fetchone()
    assert "Customer Data" in metadata[0]

def test_impact_analysis(lineage_manager):
    lineage_manager.add_lineage_edge("table_a", "table_b")
    lineage_manager.add_lineage_edge("table_b", "table_c")
    impact = lineage_manager.analyze_schema_change_impact("table_a", ["drop_column:id"])
    assert impact["dependent_count"] == 2
    assert "table_b" in impact["affected_assets"]
    assert "table_c" in impact["affected_assets"]

def test_policy_application(governance_api):
    policy = GovernancePolicy(
        policy_id="pol_001",
        name="PII Protection",
        description="Protect PII data",
        rules={"classification": "PII", "masking": "hash"}
    )
    governance_api.register_policy(policy)
    result = governance_api.apply_policy_to_asset("sensitive_table", "pol_001")
    assert result["success"] is True

def test_visualization_export(lineage_manager):
    lineage_manager.add_lineage_edge("a", "b")
    lineage_manager.apply_classification("a", "PII")
    viz_data = lineage_manager.export_visualization_data()
    assert len(viz_data["nodes"]) == 2
    assert len(viz_data["edges"]) == 1

def test_what_if_column_drop(lineage_manager, impact_viz):
    lineage_manager.add_lineage_edge("source", "target", "SELECT email FROM source")
    impact = impact_viz.what_if_column_drop("source", "email")
    assert impact["total_affected"] >= 0
    assert "email" in impact["change"]

def test_blast_radius(lineage_manager, impact_viz):
    lineage_manager.add_lineage_edge("root", "child1")
    lineage_manager.add_lineage_edge("root", "child2")
    lineage_manager.add_lineage_edge("child1", "grandchild")
    radius = impact_viz.calculate_blast_radius("root")
    assert radius["blast_radius"] == 3
    assert radius["max_depth"] == 2

def test_detect_lineage_issues(lineage_manager, governance_api):
    lineage_manager.add_lineage_edge("a", "b")
    lineage_manager.add_lineage_edge("a", "b")
    issues = governance_api.detect_lineage_issues()
    assert len(issues) > 0
    assert any(issue["type"] == "duplicate_edge" for issue in issues)
