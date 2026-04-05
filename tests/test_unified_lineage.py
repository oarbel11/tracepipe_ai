import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.workspace_lineage_aggregator import WorkspaceLineageAggregator
from scripts.peer_review.unified_lineage_context import UnifiedLineageContext


@pytest.fixture
def sample_workspace_objects():
    return {
        'ws1': [
            {'type': 'notebooks', 'data': {'object_id': 'nb1', 'path': '/Users/test/notebook1', 'created_by': 'user1'}},
            {'type': 'jobs', 'data': {'job_id': 'job1', 'name': 'ETL Job', 'notebook_path': '/Users/test/notebook1'}}
        ],
        'ws2': [
            {'type': 'dashboards', 'data': {'dashboard_id': 'dash1', 'name': 'Analytics Dashboard', 'created_by': 'user2'}}
        ]
    }


def test_aggregator_add_objects(sample_workspace_objects):
    agg = WorkspaceLineageAggregator()
    agg.add_workspace_objects(sample_workspace_objects)
    
    assert len(agg.object_metadata) == 3
    assert 'ws1' in agg.workspace_index
    assert len(agg.workspace_index['ws1']) == 2


def test_lineage_edge_creation(sample_workspace_objects):
    agg = WorkspaceLineageAggregator()
    agg.add_workspace_objects(sample_workspace_objects)
    
    source_id = 'ws1:job:job1'
    target_id = 'ws1:notebook:nb1'
    agg.add_lineage_edge(source_id, target_id, 'executes')
    
    assert agg.lineage_graph.has_edge(source_id, target_id)


def test_unified_lineage_retrieval(sample_workspace_objects):
    agg = WorkspaceLineageAggregator()
    agg.add_workspace_objects(sample_workspace_objects)
    agg.add_lineage_edge('ws1:job:job1', 'ws1:notebook:nb1', 'executes')
    
    lineage = agg.get_unified_lineage('ws1:notebook:nb1')
    
    assert 'object_id' in lineage
    assert 'upstream' in lineage
    assert len(lineage['upstream']) == 1


def test_cross_workspace_detection(sample_workspace_objects):
    agg = WorkspaceLineageAggregator()
    agg.add_workspace_objects(sample_workspace_objects)
    agg.add_lineage_edge('ws1:notebook:nb1', 'ws2:dashboard:dash1', 'feeds')
    
    cross_workspace = agg._detect_cross_workspace('ws1:notebook:nb1')
    assert cross_workspace is True
    
    flows = agg.get_all_cross_workspace_flows()
    assert len(flows) == 1


def test_impact_score_calculation(sample_workspace_objects):
    agg = WorkspaceLineageAggregator()
    agg.add_workspace_objects(sample_workspace_objects)
    agg.add_lineage_edge('ws1:notebook:nb1', 'ws2:dashboard:dash1', 'feeds')
    
    context = UnifiedLineageContext.__new__(UnifiedLineageContext)
    context.aggregator = agg
    context._initialized = True
    
    score = context._calculate_impact_score('ws1:notebook:nb1')
    assert score > 0
