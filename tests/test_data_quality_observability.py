import pytest
import networkx as nx
from datetime import datetime, timedelta
from scripts.data_quality.quality_monitor import DataQualityMonitor, QualityIssue
from scripts.data_quality.lineage_integrator import LineageQualityIntegrator


@pytest.fixture
def quality_monitor():
    monitor = DataQualityMonitor()
    monitor.conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            timestamp TIMESTAMP
        )
    """)
    old_date = datetime.now() - timedelta(hours=48)
    monitor.conn.execute(f"""
        INSERT INTO test_table VALUES (1, '{old_date}')
    """)
    return monitor


@pytest.fixture
def lineage_graph():
    graph = nx.DiGraph()
    graph.add_node('source_table')
    graph.add_node('intermediate_table')
    graph.add_node('final_table')
    graph.add_edge('source_table', 'intermediate_table')
    graph.add_edge('intermediate_table', 'final_table')
    return graph


def test_freshness_check(quality_monitor):
    issue = quality_monitor.check_freshness('test_table', max_age_hours=24)
    assert issue is not None
    assert issue.issue_type == 'freshness'
    assert issue.severity in ['high', 'medium']


def test_quality_graph_integration(quality_monitor, lineage_graph):
    integrator = LineageQualityIntegrator(quality_monitor)
    
    issues = [
        QualityIssue(
            asset_name='source_table',
            issue_type='freshness',
            description='Data is stale',
            severity='high',
            detected_at=datetime.now(),
            affected_downstream=[]
        )
    ]
    
    enhanced_graph = integrator.build_quality_graph(lineage_graph, issues)
    
    assert enhanced_graph.nodes['source_table']['quality_status'] == 'unhealthy'
    assert enhanced_graph.nodes['intermediate_table']['quality_status'] == 'at_risk'
    assert enhanced_graph.nodes['final_table']['quality_status'] == 'at_risk'


def test_quality_summary(quality_monitor, lineage_graph):
    integrator = LineageQualityIntegrator(quality_monitor)
    issues = [
        QualityIssue('source_table', 'volume', 'Volume drop', 'medium', datetime.now(), [])
    ]
    enhanced_graph = integrator.build_quality_graph(lineage_graph, issues)
    summary = integrator.get_quality_summary(enhanced_graph)
    
    assert summary['unhealthy'] == 1
    assert summary['at_risk'] == 2
    assert summary['total_issues'] > 0


def test_critical_path_identification(quality_monitor, lineage_graph):
    integrator = LineageQualityIntegrator(quality_monitor)
    issues = [
        QualityIssue('source_table', 'freshness', 'Stale', 'high', datetime.now(), [])
    ]
    enhanced_graph = integrator.build_quality_graph(lineage_graph, issues)
    critical = integrator.get_critical_path(enhanced_graph)
    
    assert 'source_table' in critical
    assert len(critical) > 0
