import pytest
from datetime import datetime, timedelta
import networkx as nx
from scripts.governance.policy_engine import GovernancePolicyEngine, PolicyViolation
from scripts.governance.alert_propagator import AlertPropagator, ImpactAlert
import tempfile
import yaml

@pytest.fixture
def test_policies():
    policies = {
        'policies': [
            {'name': 'test_schema', 'type': 'schema_drift', 'severity': 'high', 'scope': ['*.gold.*']},
            {'name': 'test_freshness', 'type': 'freshness', 'severity': 'critical', 'max_age_hours': 24, 'scope': ['*']},
            {'name': 'test_pii', 'type': 'pii_detection', 'severity': 'critical', 'patterns': ['email', 'ssn'], 'scope': ['*']}
        ]
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(policies, f)
        return f.name

@pytest.fixture
def lineage_graph():
    g = nx.DiGraph()
    g.add_node('bronze.raw', owner='data_eng')
    g.add_node('silver.clean', owner='analytics')
    g.add_node('gold.report', owner='bi_team')
    g.add_edge('bronze.raw', 'silver.clean')
    g.add_edge('silver.clean', 'gold.report')
    return g

def test_policy_engine_schema_drift(test_policies):
    engine = GovernancePolicyEngine(test_policies)
    metadata = {'schema_changed': True, 'old_schema': 'a,b', 'new_schema': 'a,b,c'}
    violations = engine.check_asset('companies_data.gold.customers', metadata)
    assert len(violations) == 1
    assert violations[0].policy_name == 'test_schema'
    assert violations[0].severity == 'high'

def test_policy_engine_freshness(test_policies):
    engine = GovernancePolicyEngine(test_policies)
    old_time = datetime.now() - timedelta(hours=48)
    metadata = {'last_updated': old_time}
    violations = engine.check_asset('companies_data.silver.orders', metadata)
    assert len(violations) >= 1
    assert any(v.policy_name == 'test_freshness' for v in violations)

def test_policy_engine_pii(test_policies):
    engine = GovernancePolicyEngine(test_policies)
    metadata = {'columns': ['user_id', 'email_address', 'order_date']}
    violations = engine.check_asset('companies_data.bronze.users', metadata)
    assert len(violations) == 1
    assert violations[0].policy_name == 'test_pii'
    assert 'email_address' in violations[0].message

def test_alert_propagator(lineage_graph):
    propagator = AlertPropagator(lineage_graph)
    violation = PolicyViolation(
        asset_id='bronze.raw',
        policy_name='test_policy',
        severity='critical',
        message='Test violation',
        detected_at=datetime.now(),
        metadata={}
    )
    alerts = propagator.propagate_violations([violation])
    assert len(alerts) == 3
    assert alerts[0].asset == 'bronze.raw'
    assert alerts[0].impact_distance == 0
    downstream_assets = [a.asset for a in alerts if a.impact_distance > 0]
    assert 'silver.clean' in downstream_assets
    assert 'gold.report' in downstream_assets

def test_severity_degradation(lineage_graph):
    propagator = AlertPropagator(lineage_graph)
    assert propagator._propagate_severity('critical', 0) == 'critical'
    assert propagator._propagate_severity('critical', 1) == 'high'
    assert propagator._propagate_severity('critical', 2) == 'medium'
    assert propagator._propagate_severity('high', 1) == 'medium'
