import pytest
import json
from src.tracepipe_ai.lineage_unification import (
    LineageUnifier,
    LineageNode,
    LineageEdge
)


def test_lineage_node_creation():
    node = LineageNode(
        node_id='ws1:table1',
        node_type='table',
        name='catalog.schema.table1',
        workspace='ws1',
        metastore='metastore1',
        metadata={'owner': 'user1'}
    )
    assert node.node_id == 'ws1:table1'
    assert node.node_type == 'table'
    assert node.workspace == 'ws1'
    node_dict = node.to_dict()
    assert node_dict['name'] == 'catalog.schema.table1'


def test_lineage_edge_creation():
    edge = LineageEdge('ws1:table1', 'ws1:table2', 'depends_on')
    assert edge.source_id == 'ws1:table1'
    assert edge.target_id == 'ws1:table2'
    edge_dict = edge.to_dict()
    assert edge_dict['edge_type'] == 'depends_on'


def test_lineage_unifier_single_workspace():
    unifier = LineageUnifier()
    workspace_config = {
        'name': 'workspace1',
        'metastore': 'metastore1',
        'lineage': {
            'nodes': [
                {'id': 'table1', 'type': 'table', 'name': 'cat.sch.tbl1'},
                {'id': 'table2', 'type': 'table', 'name': 'cat.sch.tbl2'}
            ],
            'edges': [
                {'source': 'table1', 'target': 'table2', 'type': 'depends_on'}
            ]
        }
    }
    unifier.ingest_workspace(workspace_config)
    graph = unifier.get_unified_graph()
    assert len(graph['nodes']) == 2
    assert len(graph['edges']) == 1
    assert graph['nodes'][0]['workspace'] == 'workspace1'


def test_lineage_unifier_multiple_workspaces():
    unifier = LineageUnifier()
    ws1_config = {
        'name': 'ws1',
        'metastore': 'ms1',
        'lineage': {
            'nodes': [{'id': 't1', 'type': 'table', 'name': 'tbl1'}],
            'edges': []
        }
    }
    ws2_config = {
        'name': 'ws2',
        'metastore': 'ms2',
        'lineage': {
            'nodes': [{'id': 't2', 'type': 'table', 'name': 'tbl2'}],
            'edges': []
        }
    }
    unifier.ingest_workspace(ws1_config)
    unifier.ingest_workspace(ws2_config)
    graph = unifier.get_unified_graph()
    assert len(graph['nodes']) == 2
    workspaces = {n['workspace'] for n in graph['nodes']}
    assert workspaces == {'ws1', 'ws2'}
