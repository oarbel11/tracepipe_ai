"""Tests for cross-system lineage stitching."""
import pytest
from scripts.lineage_stitcher import LineageStitcher
from scripts.external_connectors import DbtConnector, TableauConnector, SalesforceConnector
from scripts.unified_lineage import UnifiedLineageBuilder, UnifiedLineageGraph


def test_lineage_stitcher_match_entities():
    stitcher = LineageStitcher()
    e1 = {'id': '1', 'name': 'sales_table', 'type': 'table'}
    e2 = {'id': '2', 'name': 'sales_table', 'type': 'table'}
    score = stitcher.match_entities(e1, e2)
    assert score >= 1.0


def test_lineage_stitcher_stitch():
    stitcher = LineageStitcher()
    lineage1 = {'entities': [{'id': '1', 'name': 'table_a', 'type': 'table'}], 'edges': []}
    lineage2 = {'entities': [{'id': '2', 'name': 'table_a', 'type': 'table'}], 'edges': []}
    mappings = stitcher.stitch(lineage1, lineage2)
    assert len(mappings) > 0


def test_lineage_stitcher_merge():
    stitcher = LineageStitcher()
    l1 = {'entities': [{'id': '1', 'name': 'a'}], 'edges': []}
    l2 = {'entities': [{'id': '2', 'name': 'b'}], 'edges': []}
    merged = stitcher.merge_lineage([l1, l2])
    assert len(merged['entities']) == 2


def test_dbt_connector():
    connector = DbtConnector({'manifest_path': None})
    lineage = connector.extract_lineage()
    assert 'entities' in lineage
    assert 'edges' in lineage


def test_tableau_connector():
    connector = TableauConnector({})
    lineage = connector.extract_lineage()
    assert len(lineage['entities']) > 0


def test_salesforce_connector():
    connector = SalesforceConnector({})
    lineage = connector.extract_lineage()
    assert len(lineage['entities']) > 0


def test_unified_lineage_graph():
    graph = UnifiedLineageGraph()
    graph.add_entity({'id': '1', 'name': 'table1'})
    graph.add_edge('1', '2')
    assert '1' in graph.entities
    assert len(graph.edges) == 1


def test_unified_lineage_graph_upstream():
    graph = UnifiedLineageGraph()
    graph.add_edge('1', '2')
    upstream = graph.get_upstream('2')
    assert '1' in upstream


def test_unified_lineage_graph_downstream():
    graph = UnifiedLineageGraph()
    graph.add_edge('1', '2')
    downstream = graph.get_downstream('1')
    assert '2' in downstream


def test_unified_lineage_builder():
    builder = UnifiedLineageBuilder()
    builder.add_connector('dbt', DbtConnector({'manifest_path': None}))
    graph = builder.build()
    assert isinstance(graph, UnifiedLineageGraph)


def test_unified_lineage_builder_with_unity_catalog():
    builder = UnifiedLineageBuilder()
    uc_lineage = {'entities': [{'id': 'uc1', 'name': 'uc_table'}], 'edges': []}
    graph = builder.build(unity_catalog_lineage=uc_lineage)
    assert 'uc1' in graph.entities
