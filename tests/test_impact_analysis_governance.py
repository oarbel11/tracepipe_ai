"""Tests for Impact Analysis & Governance Policy Overlay."""
import pytest
from scripts.peer_review.lineage_graph import LineageGraph, LineageNode
from scripts.peer_review.impact_analysis import ImpactAnalysisEngine, ImpactNode
from scripts.peer_review.governance_policy import GovernancePolicyEngine, GovernancePolicy

def test_lineage_graph_creation():
    graph = LineageGraph()
    node1 = LineageNode('table1', 'table', {'tags': ['PII'], 'owner': 'team_a'})
    graph.add_node(node1)
    assert graph.get_node('table1') == node1

def test_downstream_impact_analysis():
    graph = LineageGraph()
    node1 = LineageNode('table1', 'table', {'tags': ['PII'], 'owner': 'team_a'})
    node2 = LineageNode('view1', 'view', {'tags': ['PII'], 'owner': 'team_b'})
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge('table1', 'view1')
    engine = ImpactAnalysisEngine(graph)
    impacts = engine.analyze_downstream_impact('table1')
    assert len(impacts) == 2
    assert impacts[0].node_id == 'table1'
    assert impacts[1].node_id == 'view1'

def test_impact_analysis_with_filters():
    graph = LineageGraph()
    node1 = LineageNode('table1', 'table', {'tags': ['PII'], 'owner': 'team_a'})
    node2 = LineageNode('view1', 'view', {'tags': ['PII'], 'owner': 'team_b'})
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_edge('table1', 'view1')
    engine = ImpactAnalysisEngine(graph)
    impacts = engine.analyze_downstream_impact('table1', {'owner': 'team_a'})
    assert len(impacts) == 1
    assert impacts[0].node_id == 'table1'

def test_governance_policy_overlay():
    graph = LineageGraph()
    node1 = LineageNode('table1', 'table', {'tags': ['PII'], 'owner': 'team_a'})
    graph.add_node(node1)
    policy_engine = GovernancePolicyEngine(graph)
    policy = GovernancePolicy('pol1', 'data_retention', {'days': 90}, ['PII'])
    policy_engine.add_policy(policy)
    policies = policy_engine.get_applicable_policies('table1')
    assert len(policies) == 1
    assert policies[0].policy_id == 'pol1'

def test_blast_radius_calculation():
    graph = LineageGraph()
    node1 = LineageNode('table1', 'table', {'tags': ['PII']})
    node2 = LineageNode('view1', 'view', {'tags': ['PII']})
    node3 = LineageNode('view2', 'view', {'tags': ['PII']})
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_node(node3)
    graph.add_edge('table1', 'view1')
    graph.add_edge('view1', 'view2')
    engine = ImpactAnalysisEngine(graph)
    radius = engine.get_blast_radius('table1')
    assert radius == 3
