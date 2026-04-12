import pytest
import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from scripts.cicd.policy_enforcer import PolicyEnforcer
from scripts.cicd.workflow_generator import generate_workflow
from scripts.peer_review.governance_policy import GovernancePolicy

def test_policy_enforcer_initialization():
    enforcer = PolicyEnforcer()
    assert len(enforcer.policies) > 0
    assert any(p.policy_id == 'pii-001' for p in enforcer.policies)

def test_enforce_policies_on_sql_files():
    enforcer = PolicyEnforcer()
    changed_files = ['etl/transform.sql', 'config/settings.yml', 'data/prod_table.sql']
    
    results = enforcer.enforce_policies(changed_files, 'test-repo')
    
    assert 'passed' in results
    assert 'violations' in results
    assert 'warnings' in results
    assert results['files_checked'] == 3
    assert results['sql_files'] == 2

def test_production_file_warning():
    enforcer = PolicyEnforcer()
    changed_files = ['etl/prod_pipeline.sql']
    
    results = enforcer.enforce_policies(changed_files, 'test-repo')
    
    assert len(results['warnings']) > 0
    assert any('prod' in w.lower() for w in results['warnings'])

def test_format_pr_comment():
    enforcer = PolicyEnforcer()
    results = {
        'passed': False,
        'violations': [{
            'policy_id': 'pii-001',
            'policy_name': 'PII Tagging',
            'file': 'users.sql',
            'severity': 'high',
            'description': 'Missing PII tags'
        }],
        'warnings': ['Production file modified'],
        'files_checked': 2,
        'sql_files': 1
    }
    
    comment = enforcer.format_pr_comment(results)
    
    assert 'Tracepipe AI Policy Check' in comment
    assert 'PII Tagging' in comment
    assert 'users.sql' in comment
    assert 'Violations' in comment

def test_policy_matching():
    policy = GovernancePolicy(
        policy_id='test-001',
        name='Test Policy',
        description='Test',
        applies_to=['prod', 'production']
    )
    
    assert policy.matches_asset([], 'prod_table')
    assert not policy.matches_asset([], 'dev_table')

def test_workflow_generator_github(tmp_path):
    output_file = tmp_path / 'workflow.yml'
    generate_workflow('github', str(output_file))
    
    assert output_file.exists()
    content = output_file.read_text()
    assert 'name: Tracepipe AI Policy Check' in content
    assert 'pull_request' in content

def test_workflow_generator_gitlab(tmp_path):
    output_file = tmp_path / 'gitlab-ci.yml'
    generate_workflow('gitlab', str(output_file))
    
    assert output_file.exists()
    content = output_file.read_text()
    assert 'tracepipe_policy_check' in content
    assert 'merge_requests' in content
