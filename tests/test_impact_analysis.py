"""Test interactive impact analysis."""
import json
from scripts.peer_review.impact_analyzer import InteractiveImpactAnalyzer


def test_impact_analyzer_initialization():
    """Test analyzer can be initialized."""
    analyzer = InteractiveImpactAnalyzer()
    assert analyzer is not None
    assert analyzer.lineage_graph == {}
    assert analyzer.asset_metadata == {}


def test_load_lineage():
    """Test loading lineage data."""
    analyzer = InteractiveImpactAnalyzer()
    lineage_data = {
        "nodes": [
            {"id": "table_a", "tags": ["PII"], "owner": "team1"},
            {"id": "table_b", "tags": ["public"], "owner": "team2"},
            {"id": "table_c", "tags": ["PII"], "owner": "team1"}
        ],
        "edges": [
            {"source": "table_a", "target": "table_b"},
            {"source": "table_b", "target": "table_c"}
        ]
    }
    analyzer.load_lineage(lineage_data)
    assert "table_a" in analyzer.lineage_graph
    assert "table_b" in analyzer.lineage_graph["table_a"]
    assert "table_a" in analyzer.asset_metadata


def test_downstream_impact_analysis():
    """Test analyzing downstream impact."""
    analyzer = InteractiveImpactAnalyzer()
    lineage_data = {
        "nodes": [
            {"id": "table_a", "tags": ["PII"]},
            {"id": "table_b", "tags": ["public"]},
            {"id": "table_c", "tags": ["PII"]}
        ],
        "edges": [
            {"source": "table_a", "target": "table_b"},
            {"source": "table_b", "target": "table_c"}
        ]
    }
    analyzer.load_lineage(lineage_data)
    result = analyzer.analyze_downstream_impact("table_a")
    assert result["source_asset"] == "table_a"
    assert result["total_impacted"] == 2
    assert len(result["impacted_assets"]) == 2


def test_filtered_impact_analysis():
    """Test impact analysis with filters."""
    analyzer = InteractiveImpactAnalyzer()
    lineage_data = {
        "nodes": [
            {"id": "table_a", "tags": ["source"]},
            {"id": "table_b", "tags": ["PII"]},
            {"id": "table_c", "tags": ["public"]}
        ],
        "edges": [
            {"source": "table_a", "target": "table_b"},
            {"source": "table_a", "target": "table_c"}
        ]
    }
    analyzer.load_lineage(lineage_data)
    result = analyzer.analyze_downstream_impact(
        "table_a", filters={"tags": ["PII"]}
    )
    assert result["total_impacted"] == 1
    assert result["impacted_assets"][0]["id"] == "table_b"


def test_nonexistent_asset():
    """Test analyzing nonexistent asset."""
    analyzer = InteractiveImpactAnalyzer()
    result = analyzer.analyze_downstream_impact("nonexistent")
    assert "error" in result
    assert result["impacted_assets"] == []
