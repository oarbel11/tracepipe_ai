"""Tests for lineage UI and governance features."""
import pytest
from scripts.lineage_ui_manager import LineageUIManager
from scripts.impact_analyzer import ImpactAnalyzer
from scripts.governance_manager import GovernanceManager


class TestLineageUI:
    def test_lineage_manager_basic(self):
        manager = LineageUIManager()
        manager.add_lineage("table_a", "table_b")
        manager.add_lineage("table_b", "table_c")

        downstream = manager.get_downstream("table_a")
        assert "table_b" in downstream
        assert "table_c" in downstream

    def test_lineage_upstream(self):
        manager = LineageUIManager()
        manager.add_lineage("table_a", "table_b")
        manager.add_lineage("table_b", "table_c")

        upstream = manager.get_upstream("table_c")
        assert "table_b" in upstream
        assert "table_a" in upstream

    def test_lineage_subgraph(self):
        manager = LineageUIManager()
        manager.add_lineage("table_a", "table_b")
        manager.add_lineage("table_b", "table_c")

        subgraph = manager.get_lineage_subgraph("table_b", depth=1)
        assert "table_a" in subgraph["nodes"]
        assert "table_b" in subgraph["nodes"]
        assert "table_c" in subgraph["nodes"]

    def test_lineage_validation(self):
        manager = LineageUIManager()
        manager.add_lineage("table_a", "table_b")
        manager.lineage_graph["isolated"] = {"upstream": [], "downstream": []}

        issues = manager.validate_lineage()
        assert len(issues) == 1
        assert issues[0]["entity"] == "isolated"


class TestImpactAnalyzer:
    def test_column_removal_impact(self):
        manager = LineageUIManager()
        manager.add_lineage("table_a", "table_b")

        analyzer = ImpactAnalyzer(manager)
        analyzer.register_schema("table_a", {"id": "int", "name": "string"})
        analyzer.register_schema("table_b", {"id": "int", "name": "string"})

        impact = analyzer.analyze_column_removal("table_a", "name")
        assert impact["column"] == "name"
        assert len(impact["impacted_entities"]) > 0

    def test_column_rename_impact(self):
        manager = LineageUIManager()
        manager.add_lineage("table_a", "table_b")

        analyzer = ImpactAnalyzer(manager)
        analyzer.register_schema("table_a", {"id": "int", "old_col": "string"})
        analyzer.register_schema("table_b", {"id": "int", "old_col": "string"})

        impact = analyzer.analyze_column_rename("table_a", "old_col", "new_col")
        assert impact["old_column"] == "old_col"
        assert impact["new_column"] == "new_col"


class TestGovernanceManager:
    def test_classification(self):
        gov = GovernanceManager()
        gov.add_classification("table_a", "ssn", "PII")
        assert gov.get_classification("table_a", "ssn") == "PII"

    def test_glossary(self):
        gov = GovernanceManager()
        gov.add_glossary_term("customer", "A person who buys products")
        term = gov.get_glossary_term("customer")
        assert term["definition"] == "A person who buys products"

    def test_masking_policy(self):
        gov = GovernanceManager()
        gov.apply_masking_policy("table_a", "ssn", "hash")
        assert gov.get_masking_policy("table_a", "ssn") == "hash"

    def test_tags(self):
        gov = GovernanceManager()
        gov.add_tag("table_a", "finance")
        gov.add_tag("table_a", "pii")
        tags = gov.get_tags("table_a")
        assert "finance" in tags
        assert "pii" in tags
