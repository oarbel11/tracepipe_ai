import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.lineage_integration import LineageEngine, SnowflakeConnector, TableauConnector
import networkx as nx

def test_snowflake_connector():
    connector = SnowflakeConnector({'system': 'snowflake', 'host': 'test.snowflakecomputing.com'})
    lineage = connector.extract_lineage()
    assert len(lineage) > 0
    assert lineage[0]['source'].startswith('snowflake')
    schema = connector.get_schema('test_table')
    assert isinstance(schema, list)

def test_tableau_connector():
    connector = TableauConnector({'system': 'tableau', 'server': 'tableau.test.com'})
    lineage = connector.extract_lineage()
    assert len(lineage) > 0
    assert 'dashboard' in lineage[0]['target']

def test_lineage_engine_basic():
    engine = LineageEngine()
    assert isinstance(engine.graph, nx.DiGraph)
    assert engine.graph.number_of_nodes() == 0

def test_add_connectors():
    engine = LineageEngine()
    engine.add_connector('snowflake', {'host': 'test.snowflakecomputing.com'})
    engine.add_connector('tableau', {'server': 'tableau.test.com'})
    assert 'snowflake' in engine.connectors
    assert 'tableau' in engine.connectors

def test_build_unified_lineage():
    engine = LineageEngine()
    engine.add_connector('snowflake', {'host': 'test.snowflakecomputing.com'})
    engine.add_connector('tableau', {'server': 'tableau.test.com'})
    graph = engine.build_unified_lineage()
    assert graph.number_of_nodes() > 0
    assert graph.number_of_edges() > 0

def test_query_lineage_downstream():
    engine = LineageEngine()
    engine.add_connector('snowflake', {'host': 'test.snowflakecomputing.com'})
    engine.build_unified_lineage()
    result = engine.query_lineage('snowflake.analytics.customers_clean', 'downstream')
    assert 'dependencies' in result
    assert isinstance(result['dependencies'], list)

def test_query_lineage_upstream():
    engine = LineageEngine()
    engine.add_connector('snowflake', {'host': 'test.snowflakecomputing.com'})
    engine.build_unified_lineage()
    result = engine.query_lineage('databricks.corporate.companies', 'upstream')
    assert result['direction'] == 'upstream'

def test_column_level_lineage():
    engine = LineageEngine()
    engine.add_connector('snowflake', {'host': 'test.snowflakecomputing.com'})
    engine.build_unified_lineage()
    assert engine.column_graph.number_of_edges() > 0

def test_query_column_lineage():
    engine = LineageEngine()
    engine.add_connector('snowflake', {'host': 'test.snowflakecomputing.com'})
    engine.build_unified_lineage()
    result = engine.query_column_lineage('snowflake.analytics.customers_clean.name')
    assert 'upstream' in result
    assert 'downstream' in result
