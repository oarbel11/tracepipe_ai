import pytest
import duckdb
from scripts.peer_review.change_simulator import ChangeSimulator, ChangeImpact
from scripts.peer_review.proactive_alerts import AlertManager, Alert
from scripts.peer_review.impact_visualizer import ImpactVisualizer

@pytest.fixture
def db_conn():
    conn = duckdb.connect(':memory:')
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    conn.execute("CREATE VIEW test_view AS SELECT * FROM test_table")
    return conn

@pytest.fixture
def simulator(db_conn):
    return ChangeSimulator(db_conn)

@pytest.fixture
def alert_manager():
    return AlertManager()

@pytest.fixture
def visualizer():
    return ImpactVisualizer()

def test_simulate_schema_change(simulator):
    changes = {'drop_column': 'name'}
    impacts = simulator.simulate_schema_change('test_table', changes)
    
    assert len(impacts) > 0
    assert impacts[0].change_type == 'drop_column'
    assert impacts[0].affected_object == 'test_table'
    assert impacts[0].severity in ['low', 'medium', 'high', 'critical']

def test_type_change_compatibility(simulator):
    changes = {'type_change': {'column': 'id', 'old_type': 'INTEGER', 'new_type': 'BIGINT'}}
    impacts = simulator.simulate_schema_change('test_table', changes)
    
    assert len(impacts) > 0
    assert impacts[0].severity == 'low'

def test_alert_registration(alert_manager):
    def test_condition(change):
        return getattr(change, 'severity', '') == 'critical'
    
    alert_manager.register_rule('critical_change', test_condition, 'critical', ['team@example.com'])
    assert len(alert_manager.alert_rules) == 1

def test_alert_evaluation(alert_manager):
    def critical_condition(change):
        return getattr(change, 'severity', '') == 'critical'
    
    alert_manager.register_rule('critical', critical_condition, 'critical', ['admin@example.com'])
    
    mock_change = ChangeImpact('drop_column', 'table1', 'table', 'critical', 5, {})
    alerts = alert_manager.evaluate_changes([mock_change])
    
    assert len(alerts) > 0
    assert alerts[0].severity == 'critical'

def test_visualization_graph(visualizer):
    mock_impacts = [
        ChangeImpact('drop_column', 'table1', 'table', 'high', 2, {'downstream': {'views': ['view1']}})
    ]
    
    graph = visualizer.build_impact_graph('root_table', mock_impacts)
    assert len(graph.nodes) >= 2
    assert len(graph.edges) >= 1

def test_blast_radius_calculation(visualizer):
    mock_impacts = [
        ChangeImpact('drop_column', 'table1', 'table', 'high', 3, {'downstream': {'views': ['view1', 'view2']}})
    ]
    
    visualizer.build_impact_graph('root', mock_impacts)
    radius = visualizer.calculate_blast_radius('root')
    
    assert 'total_affected' in radius
    assert 'by_severity' in radius
    assert radius['total_affected'] >= 0
