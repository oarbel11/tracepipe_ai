import pytest
from scripts.governance_engine import GovernanceEngine
from scripts.peer_review.governance_policy import GovernancePolicy
from scripts.anomaly_detector import AnomalyDetector

def test_governance_policy_creation():
    policy = GovernancePolicy(
        policy_id='test_policy_1',
        name='Test Schema Policy',
        description='Test policy for schema validation',
        rules={'required_columns': 'id,name,email'},
        severity='high'
    )
    assert policy.policy_id == 'test_policy_1'
    assert policy.severity == 'high'
    assert policy.enabled is True

def test_policy_matches_asset():
    policy = GovernancePolicy(
        policy_id='p1',
        name='Tagged Policy',
        description='Test',
        tags=['pii', 'customer']
    )
    assert policy.matches_asset(['pii', 'sales'], 'asset1') is True
    assert policy.matches_asset(['sales'], 'asset1') is False

def test_governance_engine_add_policy():
    engine = GovernanceEngine()
    policy = GovernancePolicy(
        policy_id='p1',
        name='Test',
        description='Test policy'
    )
    engine.add_policy(policy)
    assert len(engine.policies) == 1

def test_schema_drift_detection():
    engine = GovernanceEngine()
    baseline = {
        'columns': ['id', 'name', 'email'],
        'column_types': {'id': 'int', 'name': 'string', 'email': 'string'}
    }
    current = {
        'columns': ['id', 'name', 'phone'],
        'column_types': {'id': 'int', 'name': 'string', 'phone': 'string'}
    }
    engine.set_baseline_schema('test_table', baseline)
    violations = engine.check_schema_drift('test_table', current)
    assert len(violations) == 1
    assert violations[0]['violation_type'] == 'schema_drift'
    assert 'email' in violations[0]['details']['removed_columns']
    assert 'phone' in violations[0]['details']['added_columns']

def test_policy_compliance_required_columns():
    engine = GovernanceEngine()
    policy = GovernancePolicy(
        policy_id='p1',
        name='Schema Policy',
        description='Test',
        rules={'required_columns': 'id,name,email'},
        severity='high'
    )
    engine.add_policy(policy)
    metadata = {'columns': ['id', 'name']}
    violations = engine.check_policy_compliance('asset1', [], metadata)
    assert len(violations) == 1
    assert violations[0]['violation_type'] == 'missing_required_columns'
    assert 'email' in violations[0]['details']['missing']

def test_policy_compliance_null_rate():
    engine = GovernanceEngine()
    policy = GovernancePolicy(
        policy_id='p2',
        name='Quality Policy',
        description='Test',
        rules={'max_null_rate': '0.05'},
        severity='medium'
    )
    engine.add_policy(policy)
    metadata = {'null_rates': {'email': 0.15, 'name': 0.02}}
    violations = engine.check_policy_compliance('asset1', [], metadata)
    assert len(violations) == 1
    assert violations[0]['violation_type'] == 'high_null_rate'
    assert 'email' in violations[0]['details']['columns']

def test_anomaly_detector_schema_changes():
    detector = AnomalyDetector()
    baseline = {
        'columns': ['a', 'b'],
        'column_types': {'a': 'int', 'b': 'string'}
    }
    current = {
        'columns': ['a', 'c'],
        'column_types': {'a': 'string', 'c': 'int'}
    }
    changes = detector.detect_schema_changes(baseline, current)
    assert 'c' in changes['added_columns']
    assert 'b' in changes['removed_columns']
    assert len(changes['type_changes']) == 1
    assert changes['type_changes'][0]['column'] == 'a'

def test_anomaly_detector_data_quality():
    detector = AnomalyDetector(sensitivity=2.0)
    historical = [
        {'row_count': 100, 'avg_value': 50},
        {'row_count': 105, 'avg_value': 52},
        {'row_count': 98, 'avg_value': 49}
    ]
    current = {'row_count': 200, 'avg_value': 51}
    anomalies = detector.detect_data_quality_anomalies(historical, current)
    assert len(anomalies) >= 1
    assert any(a['metric'] == 'row_count' for a in anomalies)

def test_governance_report_generation():
    engine = GovernanceEngine()
    engine.violations = [
        {'severity': 'high', 'type': 'schema_drift'},
        {'severity': 'medium', 'type': 'quality'}
    ]
    report = engine.generate_report()
    assert report['total_violations'] == 2
    assert 'by_severity' in report
    assert 'generated_at' in report
