import pytest
from tracepipe_ai.governance import (
    PolicyEngine, ViolationDetector, AlertPropagator
)

@pytest.fixture
def policy_engine():
    return PolicyEngine()

@pytest.fixture
def violation_detector():
    return ViolationDetector()

@pytest.fixture
def lineage_graph():
    return {
        'table_a': ['table_b', 'table_c'],
        'table_b': ['table_d'],
        'table_c': ['table_d']
    }

@pytest.fixture
def alert_propagator(lineage_graph):
    return AlertPropagator(lineage_graph)

def test_create_policy(policy_engine):
    policy = policy_engine.create_policy(
        'p1', 'table_a', 'schema_change', {'strict': True})
    assert policy.policy_id == 'p1'
    assert policy.asset_id == 'table_a'
    assert policy.enabled is True

def test_schema_change_detection(violation_detector, policy_engine):
    policy = policy_engine.create_policy(
        'p1', 'table_a', 'schema_change', {})
    asset_data = {'current_schema': ['col1', 'col2'],
                  'previous_schema': ['col1']}
    violation = violation_detector.detect_violations(policy, asset_data)
    assert violation is not None
    assert violation.violation_type == 'schema_change'

def test_freshness_detection(violation_detector, policy_engine):
    policy = policy_engine.create_policy(
        'p2', 'table_a', 'freshness', {'max_hours': 24})
    asset_data = {'last_update_hours': 48}
    violation = violation_detector.detect_violations(policy, asset_data)
    assert violation is not None
    assert violation.violation_type == 'freshness'

def test_pii_detection(violation_detector, policy_engine):
    policy = policy_engine.create_policy(
        'p3', 'table_a', 'pii_detection', {})
    asset_data = {'data_sample': 'Email: test@example.com'}
    violation = violation_detector.detect_violations(policy, asset_data)
    assert violation is not None
    assert 'email' in violation.details['detected']

def test_downstream_propagation(alert_propagator, policy_engine):
    from tracepipe_ai.governance.violation_detector import Violation
    violation = Violation('p1', 'table_a', 'schema_change', {})
    owners = {'table_a': ['owner1'], 'table_b': ['owner2'],
              'table_c': ['owner3'], 'table_d': ['owner4']}
    alerts = alert_propagator.propagate_alert(violation, owners)
    assert len(alerts) == 4
    assert alerts[0].impact_level == 'critical'
    assert all(a.impact_level == 'warning' for a in alerts[1:])
