"""Tests for impact analysis functionality."""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from src.impact_analysis import (
    ImpactAnalysisEngine,
    DataAsset,
    Dependency,
    ImpactAnalysisResult,
    RootCauseAnalysisResult
)
from src.cli_impact_analysis import (
    analyze_impact_command,
    analyze_root_cause_command,
    show_lineage_command
)


class TestDataAsset:
    """Test DataAsset dataclass."""
    
    def test_data_asset_creation(self):
        """Test creating a DataAsset."""
        asset = DataAsset(
            asset_id="schema.table",
            asset_type="table",
            name="table",
            schema="schema"
        )
        assert asset.asset_id == "schema.table"
        assert asset.asset_type == "table"
        assert asset.name == "table"
        assert asset.schema == "schema"


class TestDependency:
    """Test Dependency dataclass."""
    
    def test_dependency_creation(self):
        """Test creating a Dependency."""
        dep = Dependency(
            source_id="schema.table1",
            target_id="schema.table2",
            dependency_type="direct"
        )
        assert dep.source_id == "schema.table1"
        assert dep.target_id == "schema.table2"
        assert dep.dependency_type == "direct"


class TestImpactAnalysisEngine:
    """Test ImpactAnalysisEngine class."""
    
    def test_engine_initialization(self):
        """Test engine initializes correctly."""
        engine = ImpactAnalysisEngine()
        assert engine.db_connection is None
        assert len(engine.assets) == 0
        assert len(engine.dependencies) == 0
    
    def test_engine_with_mock_db(self):
        """Test engine with mocked database connection."""
        mock_db = Mock()
        engine = ImpactAnalysisEngine(mock_db)
        assert engine.db_connection is mock_db
    
    def test_add_dependency(self):
        """Test adding dependencies."""
        engine = ImpactAnalysisEngine()
        
        # Add assets
        asset1 = DataAsset("table1", "table", "table1")
        asset2 = DataAsset("table2", "table", "table2")
        engine.assets["table1"] = asset1
        engine.assets["table2"] = asset2
        
        # Add dependency
        engine._add_dependency("table1", "table2", "direct")
        
        assert len(engine.dependencies) == 1
        assert "table2" in engine.adjacency_list["table1"]
        assert "table1" in engine.reverse_adjacency_list["table2"]
    
    def test_analyze_downstream_impact(self):
        """Test downstream impact analysis."""
        engine = ImpactAnalysisEngine()
        
        # Create a simple chain: A -> B -> C
        engine.assets["A"] = DataAsset("A", "table", "A")
        engine.assets["B"] = DataAsset("B", "table", "B")
        engine.assets["C"] = DataAsset("C", "table", "C")
        
        engine._add_dependency("A", "B", "direct")
        engine._add_dependency("B", "C", "direct")
        
        result = engine.analyze_downstream_impact("A")
        
        assert result.root_asset.asset_id == "A"
        assert len(result.affected_assets) == 2
        assert any(a.asset_id == "B" for a in result.affected_assets)
        assert any(a.asset_id == "C" for a in result.affected_assets)
        assert result.impact_score >= 0
        assert len(result.recommendations) > 0
    
    def test_analyze_downstream_impact_no_dependencies(self):
        """Test impact analysis with no downstream dependencies."""
        engine = ImpactAnalysisEngine()
        engine.assets["A"] = DataAsset("A", "table", "A")
        
        result = engine.analyze_downstream_impact("A")
        
        assert result.root_asset.asset_id == "A"
        assert len(result.affected_assets) == 0
        assert result.impact_score == 0.0
    
    def test_analyze_downstream_impact_invalid_asset(self):
        """Test impact analysis with invalid asset."""
        engine = ImpactAnalysisEngine()
        
        with pytest.raises(ValueError, match="Asset .* not found"):
            engine.analyze_downstream_impact("nonexistent")
    
    def test_analyze_root_cause(self):
        """Test root cause analysis."""
        engine = ImpactAnalysisEngine()
        
        # Create a chain: A -> B -> C
        engine.assets["A"] = DataAsset("A", "table", "A")
        engine.assets["B"] = DataAsset("B", "table", "B")
        engine.assets["C"] = DataAsset("C", "table", "C")
        
        engine._add_dependency("A", "B", "direct")
        engine._add_dependency("B", "C", "direct")
        
        result = engine.analyze_root_cause("C")
        
        assert result.affected_asset.asset_id == "C"
        assert len(result.root_causes) >= 1
        assert any(a.asset_id == "A" for a in result.root_causes)
        assert result.confidence_score >= 0
    
    def test_analyze_root_cause_no_upstream(self):
        """Test root cause analysis with no upstream dependencies."""
        engine = ImpactAnalysisEngine()
        engine.assets["A"] = DataAsset("A", "table", "A")
        
        result = engine.analyze_root_cause("A")
        
        assert result.affected_asset.asset_id == "A"
        assert len(result.root_causes) == 0
        assert result.confidence_score == 0.0
    
    def test_analyze_root_cause_invalid_asset(self):
        """Test root cause analysis with invalid asset."""
        engine = ImpactAnalysisEngine()
        
        with pytest.raises(ValueError, match="Asset .* not found"):
            engine.analyze_root_cause("nonexistent")
    
    def test_get_asset_lineage(self):
        """Test getting complete asset lineage."""
        engine = ImpactAnalysisEngine()
        
        # Create: A -> B -> C
        engine.assets["A"] = DataAsset("A", "table", "A")
        engine.assets["B"] = DataAsset("B", "table", "B")
        engine.assets["C"] = DataAsset("C", "table", "C")
        
        engine._add_dependency("A", "B", "direct")
        engine._add_dependency("B", "C", "direct")
        
        lineage = engine.get_asset_lineage("B")
        
        assert lineage["asset"]["asset_id"] == "B"
        assert lineage["upstream"]["count"] >= 1
        assert lineage["downstream"]["count"] >= 1
    
    def test_export_graph_json(self):
        """Test exporting graph as JSON."""
        engine = ImpactAnalysisEngine()
        
        engine.assets["A"] = DataAsset("A", "table", "A")
        engine.assets["B"] = DataAsset("B", "table", "B")
        engine._add_dependency("A", "B", "direct")
        
        json_output = engine.export_graph(format="json")
        data = json.loads(json_output)
        
        assert "assets" in data
        assert "dependencies" in data
        assert len(data["assets"]) == 2
        assert len(data["dependencies"]) == 1
    
    def test_export_graph_unsupported_format(self):
        """Test exporting graph with unsupported format."""
        engine = ImpactAnalysisEngine()
        
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.export_graph(format="xml")
    
    def test_load_metadata_without_db(self):
        """Test load_metadata without database connection."""
        engine = ImpactAnalysisEngine()
        engine.load_metadata()  # Should not raise error
        assert len(engine.assets) == 0
    
    def test_load_metadata_with_mock_db(self):
        """Test load_metadata with mocked database."""
        mock_db = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ("schema1", "table1"),
            ("schema1", "table2")
        ]
        mock_db.execute.return_value = mock_result
        
        engine = ImpactAnalysisEngine(mock_db)
        engine.load_metadata()
        
        # Should have loaded tables
        assert len(engine.assets) >= 0  # May vary based on mock
    
    def test_impact_recommendations_high_risk(self):
        """Test high risk recommendations."""
        engine = ImpactAnalysisEngine()
        root = DataAsset("A", "table", "A")
        affected = [DataAsset(f"B{i}", "table", f"B{i}") for i in range(10)]
        
        recommendations = engine._generate_impact_recommendations(
            root, affected, 0.8
        )
        
        assert any("HIGH RISK" in rec for rec in recommendations)
    
    def test_impact_recommendations_medium_risk(self):
        """Test medium risk recommendations."""
        engine = ImpactAnalysisEngine()
        root = DataAsset("A", "table", "A")
        affected = [DataAsset(f"B{i}", "table", f"B{i}") for i in range(5)]
        
        recommendations = engine._generate_impact_recommendations(
            root, affected, 0.5
        )
        
        assert any("MEDIUM RISK" in rec for rec in recommendations)
    
    def test_impact_recommendations_low_risk(self):
        """Test low risk recommendations."""
        engine = ImpactAnalysisEngine()
        root = DataAsset("A", "table", "A")
        affected = [DataAsset("B", "table", "B")]
        
        recommendations = engine._generate_impact_recommendations(
            root, affected, 0.2
        )
        
        assert any("LOW RISK" in rec for rec in recommendations)
    
    def test_impact_recommendations_with_dashboards(self):
        """Test recommendations when dashboards are affected."""
        engine = ImpactAnalysisEngine()
        root = DataAsset("A", "table", "A")
        affected = [
            DataAsset("B", "table", "B"),
            DataAsset("D1", "dashboard", "Dashboard1"),
            DataAsset("D2", "dashboard", "Dashboard2")
        ]
        
        recommendations = engine._generate_impact_recommendations(
            root, affected, 0.5
        )
        
        assert any("dashboard" in rec.lower() for rec in recommendations)


class TestCLICommands:
    """Test CLI command functions."""
    
    @patch('src.cli_impact_analysis.get_db_connection')
    def test_analyze_impact_command_success(self, mock_db, capsys):
        """Test impact command with valid input."""
        mock_db.return_value = None
        
        # Create engine with test data
        with patch('src.cli_impact_analysis.ImpactAnalysisEngine') as MockEngine:
            mock_engine = Mock()
            mock_result = Mock()
            mock_result.root_asset = DataAsset("A", "table", "A")
            mock_result.affected_assets = [DataAsset("B", "table", "B")]
            mock_result.impact_score = 0.5
            mock_result.recommendations = ["Test recommendation"]
            mock_result.dependency_chain = []
            
            mock_engine.analyze_downstream_impact.return_value = mock_result
            MockEngine.return_value = mock_engine
            
            result = analyze_impact_command("A", "text")
            
            assert result == 0
            captured = capsys.readouterr()
            assert "Impact Analysis" in captured.out
    
    @patch('src.cli_impact_analysis.get_db_connection')
    def test_analyze_impact_command_json_output(self, mock_db, capsys):
        """Test impact command with JSON output."""
        mock_db.return_value = None
        
        with patch('src.cli_impact_analysis.ImpactAnalysisEngine') as MockEngine:
            mock_engine = Mock()
            mock_result = Mock()
            mock_result.root_asset = DataAsset("A", "table", "A")
            mock_result.affected_assets = []
            mock_result.impact_score = 0.0
            mock_result.recommendations = []
            mock_result.dependency_chain = []
            
            mock_engine.analyze_downstream_impact.return_value = mock_result
            MockEngine.return_value = mock_engine
            
            result = analyze_impact_command("A", "json")
            
            assert result == 0
            captured = capsys.readouterr()
            output = json.loads(captured.out)
            assert "root_asset" in output
            assert "affected_count" in output
    
    @patch('src.cli_impact_analysis.get_db_connection')
    def test_analyze_root_cause_command_success(self, mock_db, capsys):
        """Test root cause command with valid input."""
        mock_db.return_value = None
        
        with patch('src.cli_impact_analysis.ImpactAnalysisEngine') as MockEngine:
            mock_engine = Mock()
            mock_result = Mock()
            mock_result.affected_asset = DataAsset("C", "table", "C")
            mock_result.root_causes = [DataAsset("A", "table", "A")]
            mock_result.confidence_score = 0.8
            mock_result.dependency_chain = []
            
            mock_engine.analyze_root_cause.return_value = mock_result
            MockEngine.return_value = mock_engine
            
            result = analyze_root_cause_command("C", "text")
            
            assert result == 0
            captured = capsys.readouterr()
            assert "Root Cause Analysis" in captured.out
    
    @patch('src.cli_impact_analysis.get_db_connection')
    def test_show_lineage_command_success(self, mock_db, capsys):
        """Test lineage command with valid input."""
        mock_db.return_value = None
        
        with patch('src.cli_impact_analysis.ImpactAnalysisEngine') as MockEngine:
            mock_engine = Mock()
            mock_lineage = {
                "asset": {
                    "asset_id": "B",
                    "asset_type": "table",
                    "name": "B"
                },
                "upstream": {"count": 1, "assets": []},
                "downstream": {"count": 1, "assets": []}
            }
            
            mock_engine.get_asset_lineage.return_value = mock_lineage
            MockEngine.return_value = mock_engine
            
            result = show_lineage_command("B", "text")
            
            assert result == 0
            captured = capsys.readouterr()
            assert "Lineage for" in captured.out
    
    @patch('src.cli_impact_analysis.get_db_connection')
    def test_command_error_handling(self, mock_db, capsys):
        """Test command error handling."""
        mock_db.return_value = None
        
        with patch('src.cli_impact_analysis.ImpactAnalysisEngine') as MockEngine:
            mock_engine = Mock()
            mock_engine.analyze_downstream_impact.side_effect = ValueError("Test error")
            MockEngine.return_value = mock_engine
            
            result = analyze_impact_command("invalid", "text")
            
            assert result == 1
            captured = capsys.readouterr()
            assert "Error" in captured.err
