import pytest
import os
from scripts.peer_review.governance_orchestrator import GovernanceOrchestrator
from scripts.peer_review.governance_policy import GovernancePolicy
from scripts.peer_review.policy_engine import PolicyEngine

@pytest.fixture
def orchestrator():
    db_path = 'test_lineage.duckdb'
    if os.path.exists(db_path):
        os.remove(db_path)
    orch = GovernanceOrchestrator(db_path)
    yield orch
    orch.close()
    if os.path.exists(db_path):
        os.remove(db_path)

def test_policy_creation():
    policy = GovernancePolicy(
        policy_id='test_policy',
        name='Test Policy',
        description='Test',
        tags=['PII'],
        severity='high'
    )
    assert policy.policy_id == 'test_policy'
    assert policy.severity == 'high'

def test_register_asset(orchestrator):
    orchestrator.register_asset(
        'table.users',
        tags=['PII', 'sensitive'],
        metadata={'compliance_zone': 'public'}
    )
    violations = orchestrator.evaluate_policies('table.users')
    assert isinstance(violations, list)

def test_policy_violation_detection(orchestrator):
    policy = GovernancePolicy(
        policy_id='pii_policy',
        name='PII Protection',
        description='Protect PII',
        tags=['PII'],
        rules={'compliance_zone': 'private'},
        severity='high'
    )
    orchestrator.add_policy(policy)
    orchestrator.register_asset(
        'table.users',
        tags=['PII'],
        metadata={'compliance_zone': 'public'}
    )
    violations = orchestrator.evaluate_policies()
    assert len(violations) > 0
    assert violations[0].asset_id == 'table.users'

def test_remediation_execution(orchestrator):
    policy = GovernancePolicy(
        policy_id='test_policy',
        name='Test',
        description='Test',
        tags=['PII'],
        severity='high'
    )
    orchestrator.add_policy(policy)
    orchestrator.register_asset('table.test', tags=['PII'], metadata={})
    violations = orchestrator.evaluate_policies()
    if violations:
        result = orchestrator.execute_remediation(violations[0], 'mask')
        assert result['status'] == 'success'
        assert result['action'] == 'mask'
