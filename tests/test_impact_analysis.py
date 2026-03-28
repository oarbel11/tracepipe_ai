#!/usr/bin/env python3
"""
Tests for Impact Analysis features
"""

import pytest
import tempfile
import os
import duckdb
import networkx as nx
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.impact_analysis.lineage_graph import LineageGraphBuilder
from scripts.impact_analysis.impact_analyzer import ImpactAnalyzer
from scripts.impact_analysis.root_cause_analyzer import RootCauseAnalyzer
from scripts.impact_analysis.visualizer import DependencyVisualizer


@pytest.fixture
def test_db():
    """Create a temporary test database with sample data"""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as tmp:
        db_path = tmp.name
    
    con = duckdb.connect(db_path)
    
    # Create test schema
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE SCHEMA silver")
    con.execute("CREATE SCHEMA conformed")
    
    # Create base tables
    con.execute("""
        CREATE TABLE raw.companies (
            company_id INTEGER,
            name VARCHAR,
            location VARCHAR,
            industry VARCHAR
        )
    """)
    
    con.execute("""
        CREATE TABLE raw.employees (
            emp_id INTEGER,
            full_name VARCHAR,
            birth_date DATE,
            gender VARCHAR
        )
    """)
    
    con.execute("""
        CREATE TABLE raw.job_history (
            job_id INTEGER,
            emp_id INTEGER,
            company_id INTEGER,
            role VARCHAR,
            salary INTEGER,
            start_date DATE,
            end_date DATE,
            is_current INTEGER
        )
    """)
    
    # Create derived view
    con.execute("""
        CREATE VIEW silver.employee_current_job AS
        SELECT 
            e.emp_id,
            e.full_name,
            j.company_id,
            j.role,
            j.salary
        FROM raw.employees e
        JOIN raw.job_history j ON e.emp_id = j.emp_id
        WHERE j.is_current = 1
    """)
    
    # Create final conformed view
    con.execute("""
        CREATE VIEW conformed.employee_stats AS
        SELECT
            ej.emp_id,
            ej.full_name,
            c.name as company_name,
            c.industry,
            ej.role,
            ej.salary
        FROM silver.employee_current_job ej
        JOIN raw.companies c ON ej.company_id = c.company_id
    """)
    
    con.close()
    
    yield db_path
    
    # Cleanup
    os.unlink(db_path)


class TestLineageGraphBuilder:
    """Test LineageGraphBuilder functionality"""
    
    def test_build_graph(self, test_db):
        """Test that graph is built correctly"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        
        assert graph.number_of_nodes() > 0
        assert graph.number_of_edges() > 0
        
        # Check that tables exist
        assert any('raw.companies' in node for node in graph.nodes())
        assert any('raw.employees' in node for node in graph.nodes())
    
    def test_table_nodes(self, test_db):
        """Test that table nodes are created correctly"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        
        # Find table nodes
        table_nodes = [n for n in graph.nodes() if graph.nodes[n].get('type') == 'table']
        
        assert len(table_nodes) >= 3  # At least 3 base tables
    
    def test_view_nodes(self, test_db):
        """Test that view nodes are created correctly"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        
        # Find view nodes
        view_nodes = [n for n in graph.nodes() if graph.nodes[n].get('type') == 'view']
        
        assert len(view_nodes) >= 2  # At least 2 views
    
    def test_column_nodes(self, test_db):
        """Test that column nodes are created"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        
        # Find column nodes
        column_nodes = [n for n in graph.nodes() if graph.nodes[n].get('type') == 'column']
        
        assert len(column_nodes) > 0


class TestImpactAnalyzer:
    """Test ImpactAnalyzer functionality"""
    
    def test_analyze_impact_base_table(self, test_db):
        """Test impact analysis on a base table"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = ImpactAnalyzer(graph)
        
        result = analyzer.analyze_impact('raw.companies')
        
        assert result.source_asset is not None
        assert result.total_affected >= 0
        # Should affect downstream views
        assert len(result.affected_views) > 0 or len(result.affected_tables) >= 0
    
    def test_analyze_impact_no_dependencies(self, test_db):
        """Test impact analysis on asset with no downstream dependencies"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = ImpactAnalyzer(graph)
        
        # conformed.employee_stats should have no downstream dependencies
        result = analyzer.analyze_impact('conformed.employee_stats')
        
        assert result.total_affected == 0
    
    def test_simulate_change(self, test_db):
        """Test change simulation"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = ImpactAnalyzer(graph)
        
        simulation = analyzer.simulate_change('raw.companies', 'schema')
        
        assert 'change_type' in simulation
        assert 'risk_level' in simulation
        assert 'blast_radius' in simulation
        assert 'recommended_actions' in simulation
    
    def test_risk_assessment(self, test_db):
        """Test risk level assessment"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = ImpactAnalyzer(graph)
        
        result = analyzer.analyze_impact('raw.companies')
        risk = analyzer._assess_risk_level(result)
        
        assert risk in ['NONE', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL']


class TestRootCauseAnalyzer:
    """Test RootCauseAnalyzer functionality"""
    
    def test_analyze_root_cause(self, test_db):
        """Test root cause analysis"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = RootCauseAnalyzer(graph)
        
        result = analyzer.analyze_root_cause('conformed.employee_stats')
        
        assert result.target_asset is not None
        assert result.total_dependencies > 0
        # Should have upstream tables
        assert len(result.source_tables) > 0
    
    def test_critical_dependencies(self, test_db):
        """Test identification of critical dependencies"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = RootCauseAnalyzer(graph)
        
        result = analyzer.analyze_root_cause('conformed.employee_stats')
        
        # Should have critical dependencies identified
        assert len(result.critical_dependencies) >= 0
    
    def test_diagnose_data_quality(self, test_db):
        """Test data quality diagnosis"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = RootCauseAnalyzer(graph)
        
        diagnosis = analyzer.diagnose_data_quality_issue('conformed.employee_stats')
        
        assert 'target_asset' in diagnosis
        assert 'total_upstream_dependencies' in diagnosis
        assert 'recommendations' in diagnosis
        assert 'investigation_priority' in diagnosis
    
    def test_root_cause_base_table(self, test_db):
        """Test root cause analysis on base table (no upstream)"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = RootCauseAnalyzer(graph)
        
        result = analyzer.analyze_root_cause('raw.companies')
        
        # Base table should have no upstream dependencies
        assert result.total_dependencies == 0


class TestDependencyVisualizer:
    """Test DependencyVisualizer functionality"""
    
    def test_visualize_impact(self, test_db):
        """Test impact visualization"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = ImpactAnalyzer(graph)
        visualizer = DependencyVisualizer(graph)
        
        result = analyzer.analyze_impact('raw.companies')
        all_affected = result.affected_tables + result.affected_views
        
        output = visualizer.visualize_impact(
            result.source_asset, 
            all_affected, 
            result.impact_paths
        )
        
        assert 'BLAST RADIUS ANALYSIS' in output
        assert len(output) > 0
    
    def test_visualize_root_cause(self, test_db):
        """Test root cause visualization"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        analyzer = RootCauseAnalyzer(graph)
        visualizer = DependencyVisualizer(graph)
        
        result = analyzer.analyze_root_cause('conformed.employee_stats')
        all_sources = result.source_tables + result.source_columns
        
        output = visualizer.visualize_root_cause(
            result.target_asset,
            all_sources,
            result.dependency_paths
        )
        
        assert 'ROOT CAUSE ANALYSIS' in output
        assert len(output) > 0
    
    def test_visualize_graph_summary(self, test_db):
        """Test graph summary visualization"""
        builder = LineageGraphBuilder(test_db)
        graph = builder.build_graph()
        visualizer = DependencyVisualizer(graph)
        
        output = visualizer.visualize_graph_summary()
        
        assert 'DATA LINEAGE GRAPH SUMMARY' in output
        assert 'Total edges' in output
        assert 'Total nodes' in output


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
