import pytest
from scripts.lineage_graph_store import LineageGraphStore
from scripts.peer_review.governance_orchestrator import GovernanceOrchestrator

def test_lineage_graph_store_basic():
    store = LineageGraphStore()
    node = store.add_node('table1', 'table', {'has_pii': True})
    assert node['id'] == 'table1'
    assert node['type'] == 'table'
    assert store.get_node('table1') is not None

def test_lineage_graph_store_edges():
    store = LineageGraphStore()
    store.add_node('table1', 'table')
    store.add_node('table2', 'table')
    edge = store.add_edge('table1', 'table2')
    assert edge['source'] == 'table1'
    assert edge['target'] == 'table2'

def test_lineage_graph_store_query():
    store = LineageGraphStore()
    store.add_node('table1', 'table', {'has_pii': True})
    store.add_node('table2', 'table', {'has_pii': False})
    results = store.query_nodes({'type': 'table', 'has_pii': True})
    assert len(results) == 1
    assert results[0]['id'] == 'table1'

def test_governance_orchestrator_initialization():
    orchestrator = GovernanceOrchestrator()
    assert orchestrator.graph_store is not None
    assert orchestrator.policy_engine is not None

def test_governance_orchestrator_pii_violation():
    orchestrator = GovernanceOrchestrator()
    orchestrator.graph_store.add_node('table1', 'table', 
        {'has_pii': True, 'compliant_location': False})
    
    policy = {
        'id': 'pii_policy_1',
        'type': 'pii_compliance',
        'target_type': 'table',
        'severity': 'high',
        'remediation': 'mask'
    }
    orchestrator.add_policy(policy)
    
    results = orchestrator.scan_and_remediate()
    assert len(results) > 0
    assert results[0]['action'] == 'mask'
    assert results[0]['status'] == 'success'

def test_governance_orchestrator_alert():
    orchestrator = GovernanceOrchestrator()
    orchestrator.graph_store.add_node('table2', 'table', 
        {'has_pii': True, 'compliant_location': False})
    
    policy = {
        'id': 'pii_policy_2',
        'type': 'pii_compliance',
        'target_type': 'table',
        'severity': 'medium',
        'remediation': 'alert'
    }
    orchestrator.add_policy(policy)
    
    results = orchestrator.scan_and_remediate()
    assert len(results) > 0
    assert results[0]['action'] == 'alert'
    assert len(orchestrator.get_alert_log()) > 0
