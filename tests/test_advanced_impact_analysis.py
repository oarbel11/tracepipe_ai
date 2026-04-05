import pytest
import networkx as nx
from scripts.peer_review.data_quality_integration import DataQualityIntegration
from scripts.peer_review.advanced_impact_analyzer import AdvancedImpactAnalyzer
from scripts.peer_review.blast_radius import ImpactAnalysisMapper

@pytest.fixture
def sample_graph():
    g = nx.DiGraph()
    g.add_edges_from([
        ('raw_orders', 'stg_orders'),
        ('stg_orders', 'fact_orders'),
        ('fact_orders', 'report_daily_sales'),
        ('fact_orders', 'report_customer_ltv')
    ])
    return g

@pytest.fixture
def quality_integration():
    return DataQualityIntegration(':memory:')

def test_quality_metric_recording(quality_integration):
    quality_integration.record_quality_metric('test_table', 'completeness', 95.0, 90.0)
    score = quality_integration.get_table_quality_score('test_table')
    assert score['status'] == 'healthy'
    assert score['score'] == 100.0

def test_observability_signals(quality_integration):
    quality_integration.record_signal('test_table', 'latency', 'warning', 'Slow query')
    signals = quality_integration.get_recent_signals('test_table')
    assert len(signals) == 1
    assert signals[0]['severity'] == 'warning'

def test_impact_analysis(sample_graph, quality_integration):
    analyzer = AdvancedImpactAnalyzer(sample_graph, quality_integration)
    impact = analyzer.analyze_change_impact('stg_orders', 'schema')
    
    assert impact['changed_table'] == 'stg_orders'
    assert impact['downstream_count'] == 3
    assert 'fact_orders' in impact['downstream_tables']

def test_whatif_simulation(sample_graph, quality_integration):
    analyzer = AdvancedImpactAnalyzer(sample_graph, quality_integration)
    result = analyzer.simulate_whatif('stg_orders', 'column_drop')
    
    assert result['scenario'] == 'column_drop'
    assert result['table'] == 'stg_orders'
    assert 'recommendation' in result

def test_blast_radius_with_quality(sample_graph):
    mapper = ImpactAnalysisMapper(sample_graph)
    quality_scores = {'fact_orders': 45.0, 'report_daily_sales': 85.0}
    
    result = mapper.get_blast_radius_with_quality('stg_orders', quality_scores)
    assert result['total_count'] == 3
    assert result['high_risk_count'] >= 1

def test_critical_paths(sample_graph, quality_integration):
    analyzer = AdvancedImpactAnalyzer(sample_graph, quality_integration)
    impact = analyzer.analyze_change_impact('raw_orders')
    
    assert len(impact['critical_paths']) > 0
    assert 'raw_orders' in impact['critical_paths'][0]

def test_quality_risk_assessment(sample_graph, quality_integration):
    quality_integration.record_quality_metric('fact_orders', 'accuracy', 40.0, 80.0)
    analyzer = AdvancedImpactAnalyzer(sample_graph, quality_integration)
    impact = analyzer.analyze_change_impact('stg_orders')
    
    assert len(impact['quality_risks']) > 0
