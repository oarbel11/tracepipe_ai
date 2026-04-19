import pytest
from scripts.peer_review.governance_policy import GovernancePolicy, PolicyViolation
from scripts.peer_review.policy_enforcement import PolicyViolationDetector, EnforcementEngine
from scripts.peer_review.interactive_enforcement import InteractiveLineageEnforcement

def test_policy_violation_detection():
    policy = GovernancePolicy(
        policy_id='PII_001',
        name='PII Protection',
        tags=['pii'],
        severity='high',
        rules={'has_masking': 'true'}
    )
    detector = PolicyViolationDetector([policy])
    assets = [
        {'id': 'users_table', 'tags': ['pii'], 'has_masking': False},
        {'id': 'orders_table', 'tags': ['transactional'], 'has_masking': False}
    ]
    violations = detector.detect_violations(assets)
    assert len(violations) == 1
    assert violations[0].asset_id == 'users_table'
    assert violations[0].policy.severity == 'high'

def test_high_risk_asset_detection():
    policy = GovernancePolicy(policy_id='TEST_001', name='Test', tags=[])
    detector = PolicyViolationDetector([policy])
    assets = [{'id': 'table_a'}, {'id': 'table_b'}]
    upstream_changes = {'table_a': ['source_1', 'source_2']}
    high_risk = detector.get_high_risk_assets(assets, upstream_changes)
    assert len(high_risk) == 1
    assert high_risk[0]['id'] == 'table_a'
    assert len(high_risk[0]['changed_dependencies']) == 2

def test_enforcement_engine_alerts():
    alert_triggered = []
    def mock_handler(alert):
        alert_triggered.append(alert)
    
    engine = EnforcementEngine()
    engine.register_alert_handler(mock_handler)
    
    policy = GovernancePolicy(policy_id='TEST', name='Test Policy', severity='medium')
    violation = PolicyViolation(policy, 'asset_1', ['Missing masking'], '2024-01-01')
    alert = engine.trigger_alert(violation)
    
    assert len(alert_triggered) == 1
    assert alert_triggered[0]['severity'] == 'medium'
    assert alert_triggered[0]['asset_id'] == 'asset_1'

def test_remediation_suggestions():
    engine = EnforcementEngine()
    policy = GovernancePolicy(policy_id='PII_001', name='PII', tags=['pii'], severity='high')
    violation = PolicyViolation(policy, 'users_table', ['No masking'], '2024-01-01')
    suggestions = engine.suggest_remediation(violation)
    assert len(suggestions) > 0
    assert suggestions[0].action_type == 'mask_data'

def test_interactive_lineage_enforcement():
    policies = [
        GovernancePolicy(policy_id='PII_001', name='PII', tags=['pii'], 
                        severity='high', rules={'has_masking': 'true'})
    ]
    enforcer = InteractiveLineageEnforcement(policies)
    assets = [{'id': 'users', 'tags': ['pii'], 'has_masking': False}]
    
    analysis = enforcer.analyze_lineage_graph(assets)
    assert analysis['total_violations'] == 1
    assert analysis['critical_violations'] == 1

def test_visualization_highlights():
    policies = [GovernancePolicy(policy_id='Q_001', name='Quality', tags=['quality'], 
                                 severity='medium', rules={'has_validation': 'true'})]
    enforcer = InteractiveLineageEnforcement(policies)
    assets = [{'id': 'orders', 'tags': ['quality'], 'has_validation': False}]
    
    highlights = enforcer.get_visualization_highlights(assets)
    assert 'orders' in highlights['violation_nodes']
    assert highlights['severity_map']['orders'] == 'medium'

def test_compliance_dashboard():
    policies = [GovernancePolicy(policy_id='P1', name='Policy 1', tags=['pii'], 
                                 severity='high', rules={'encrypted': 'true'})]
    enforcer = InteractiveLineageEnforcement(policies)
    assets = [
        {'id': 'a1', 'tags': ['pii'], 'encrypted': True},
        {'id': 'a2', 'tags': ['pii'], 'encrypted': False}
    ]
    dashboard = enforcer.get_compliance_dashboard(assets)
    assert dashboard['total_assets'] == 2
    assert dashboard['compliant_assets'] == 1
    assert dashboard['compliance_rate'] == 50.0
    assert dashboard['requires_immediate_action'] == 1
