import pytest
from src.cicd_integration import GitCICDIntegration
from src.impact_analysis import InteractiveImpactAnalyzer


def test_git_cicd_workflow():
    integration = GitCICDIntegration({'policy_checks': True})
    commit_data = {
        'files': ['pipeline1.sql', 'pipeline2.py'],
        'author': 'dev@example.com'
    }
    result = integration.process_commit(commit_data)
    assert 'status' in result
    assert 'analysis' in result
    assert 'policy_result' in result


def test_policy_enforcement():
    integration = GitCICDIntegration()
    high_impact = {'files': [f'file{i}.py' for i in range(10)]}
    result = integration.process_commit(high_impact)
    assert result['status'] == 'failed'
    assert not result['policy_result']['passed']


def test_interactive_analysis():
    analyzer = InteractiveImpactAnalyzer()
    changes = {'files': ['data_pipeline.sql']}
    analysis = analyzer.analyze_changes(changes)
    assert analysis['impact_score'] > 0
    report = analyzer.get_report()
    assert report['status'] == 'success'
