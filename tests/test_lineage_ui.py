import pytest
import json
from scripts.lineage_ui_manager import LineageUIManager
from scripts.governance_ui_manager import GovernanceUIManager
from scripts.impact_analysis_visualizer import ImpactAnalysisVisualizer

def test_lineage_manager_add_and_get():
    manager = LineageUIManager()
    lid = manager.add_lineage('table_a', 'table_b', {'type': 'transform'})
    assert lid > 0
    lineage = manager.get_lineage('table_a')
    assert 'table_b' in lineage['downstream']

def test_lineage_persistence():
    manager = LineageUIManager()
    manager.add_lineage('source1', 'target1')
    manager.add_lineage('source2', 'target2')
    all_lineage = manager.get_all_lineage()
    assert len(all_lineage) == 2

def test_lineage_delete():
    manager = LineageUIManager()
    lid = manager.add_lineage('a', 'b')
    assert manager.delete_lineage(lid)
    assert not manager.delete_lineage(lid)

def test_lineage_issue_detection():
    manager = LineageUIManager()
    manager.add_lineage('a', 'b')
    manager.add_lineage('b', 'c')
    manager.add_lineage('c', 'a')
    issues = manager.detect_lineage_issues()
    assert len(issues) > 0

def test_lineage_export_import():
    manager = LineageUIManager()
    manager.add_lineage('x', 'y')
    exported = manager.export_lineage()
    new_manager = LineageUIManager()
    assert new_manager.import_lineage(exported)

def test_governance_policy_management():
    gov = GovernanceUIManager()
    pid = gov.add_policy('mask_pii', 'masking', {'columns': ['ssn', 'email']})
    assert pid > 0
    policy = gov.get_policy(pid)
    assert policy['name'] == 'mask_pii'
    assert gov.update_policy(pid, {'enabled': False})
    assert gov.delete_policy(pid)

def test_governance_tags():
    gov = GovernanceUIManager()
    assert gov.apply_tag('table1', 'pii', True)
    tags = gov.get_tags('table1')
    assert tags['pii'] is True
    assert gov.remove_tag('table1', 'pii')

def test_governance_classification():
    gov = GovernanceUIManager()
    assert gov.set_classification('table1', 'confidential')
    assert gov.get_classification('table1') == 'confidential'

def test_governance_glossary():
    gov = GovernanceUIManager()
    assert gov.add_glossary_term('revenue', 'Total income')
    term = gov.get_glossary_term('revenue')
    assert term['definition'] == 'Total income'

def test_impact_analysis():
    manager = LineageUIManager()
    manager.add_lineage('a', 'b')
    manager.add_lineage('b', 'c')
    visualizer = ImpactAnalysisVisualizer(manager)
    impact = visualizer.analyze_impact('a', 'schema_change')
    assert impact['impact_count'] >= 0

def test_blast_radius():
    manager = LineageUIManager()
    manager.add_lineage('root', 'child1')
    manager.add_lineage('root', 'child2')
    visualizer = ImpactAnalysisVisualizer(manager)
    radius = visualizer.calculate_blast_radius('root')
    assert radius['direct_downstream'] == 2
