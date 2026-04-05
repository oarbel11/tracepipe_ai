import pytest
import json
import os
from scripts.impact_simulator import ImpactSimulator
from scripts.peer_review.blast_radius import ImpactAnalysisMapper


class TestImpactSimulator:
    def setup_method(self):
        self.simulator = ImpactSimulator()
    
    def test_simulate_schema_change(self):
        result = self.simulator.simulate_change(
            target='companies_data.corporate.companies',
            change_type='schema_change',
            change_params={'column': 'revenue'}
        )
        
        assert result['target'] == 'companies_data.corporate.companies'
        assert result['change_type'] == 'schema_change'
        assert 'affected_count' in result
        assert 'risk_score' in result
        assert isinstance(result['affected_assets'], list)
        assert 0.0 <= result['risk_score'] <= 1.0
    
    def test_simulate_drop_column(self):
        result = self.simulator.simulate_change(
            target='test.schema.table',
            change_type='drop_column',
            change_params={'column': 'email'}
        )
        
        assert result['change_type'] == 'drop_column'
        assert result['risk_score'] >= 0.0
    
    def test_graph_export(self):
        result = self.simulator.simulate_change(
            target='test.table',
            change_type='deprecate'
        )
        
        graph_data = result['graph_data']
        assert 'nodes' in graph_data
        assert 'edges' in graph_data
        assert isinstance(graph_data['nodes'], list)
        assert isinstance(graph_data['edges'], list)
    
    def test_visualize_graph(self, tmp_path):
        result = self.simulator.simulate_change(
            target='test.table',
            change_type='data_quality'
        )
        
        output_file = tmp_path / 'impact.html'
        self.simulator.visualize_graph(result, output=str(output_file))
        
        assert output_file.exists()
        content = output_file.read_text()
        assert 'Impact Simulation' in content
        assert 'd3.forceSimulation' in content
    
    def test_risk_calculation(self):
        high_risk = self.simulator.simulate_change(
            target='critical.table',
            change_type='deprecate'
        )
        
        low_risk = self.simulator.simulate_change(
            target='test.table',
            change_type='data_quality'
        )
        
        assert high_risk['risk_score'] >= low_risk['risk_score']


class TestImpactAnalysisMapper:
    def setup_method(self):
        self.mapper = ImpactAnalysisMapper()
    
    def test_get_downstream_impact(self):
        impact = self.mapper.get_downstream_impact('test.table')
        
        assert 'tables' in impact
        assert 'dashboards' in impact
        assert 'ml_models' in impact
        assert isinstance(impact['tables'], list)
    
    def test_blast_radius_score(self):
        score = self.mapper.calculate_blast_radius_score('test.table')
        assert 0.0 <= score <= 1.0
