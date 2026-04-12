import pytest
from scripts.peer_review.policy_enforcer import PolicyEnforcer, EnforcementResult
from scripts.peer_review.governance_policy import GovernancePolicy
from scripts.peer_review.lineage_graph_optimizer import (
    LineageGraphOptimizer, ViewportWindow
)

def test_tag_propagation():
    policy = GovernancePolicy(
        policy_id='pii_001',
        name='PII Propagation',
        description='Test',
        tags=['PII'],
        rules={'action': 'propagate'}
    )
    enforcer = PolicyEnforcer([policy])
    lineage = {'table_a': ['table_b'], 'table_b': ['table_c']}
    asset_tags = {'table_a': ['PII']}
    
    propagated = enforcer.propagate_tags(lineage, asset_tags)
    
    assert 'PII' in propagated['table_a']
    assert 'PII' in propagated['table_b']
    assert 'PII' in propagated['table_c']

def test_policy_enforcement():
    policy = GovernancePolicy(
        policy_id='pii_002',
        name='PII Restrict',
        description='Test',
        tags=['PII'],
        rules={'action': 'restrict', 'access_level': 'admin_only'}
    )
    enforcer = PolicyEnforcer([policy])
    lineage = {'table_a': ['table_b']}
    asset_tags = {'table_a': ['PII']}
    
    results = enforcer.enforce_policies(lineage, asset_tags)
    
    assert len(results) > 0
    pii_results = [r for r in results if 'PII' in r.tags_applied]
    assert len(pii_results) > 0
    assert pii_results[0].access_level == 'admin_only'

def test_graph_clustering():
    optimizer = LineageGraphOptimizer(max_viewport_nodes=50)
    lineage = {
        'a': ['b', 'c'],
        'b': ['d'],
        'c': ['e'],
        'd': ['f'],
        'e': ['g']
    }
    
    clusters = optimizer.build_clusters(lineage, cluster_size=3)
    
    assert len(clusters) > 0
    total_nodes = sum(len(c.nodes) for c in clusters.values())
    assert total_nodes >= 5

def test_viewport_subgraph():
    optimizer = LineageGraphOptimizer(max_viewport_nodes=10)
    lineage = {
        'a': ['b', 'c'],
        'b': ['d'],
        'c': ['e'],
        'd': ['f']
    }
    viewport = ViewportWindow(center_node='b', max_depth=2, max_nodes=10)
    
    subgraph = optimizer.get_viewport_subgraph(lineage, viewport)
    
    assert 'b' in [n['id'] for n in subgraph['nodes']]
    assert subgraph['metadata']['total_nodes'] <= 10
    assert len(subgraph['edges']) > 0

def test_policy_validation():
    policy = GovernancePolicy(
        policy_id='test_001',
        name='Test',
        description='Test',
        rules={'action': 'invalid_action'}
    )
    
    errors = policy.validate_rules()
    assert len(errors) > 0
    assert 'Invalid action' in errors[0]
