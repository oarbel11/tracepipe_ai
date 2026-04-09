import pytest
import os
import tempfile
import networkx as nx
from scripts.peer_review.business_glossary import BusinessGlossary
from scripts.peer_review.semantic_lineage import SemanticLineageMapper
from scripts.peer_review.context_builder import ContextBuilder


@pytest.fixture
def temp_glossary():
    fd, path = tempfile.mkstemp(suffix='.json')
    os.close(fd)
    glossary = BusinessGlossary(path)
    yield glossary
    os.unlink(path)


def test_add_and_retrieve_term(temp_glossary):
    temp_glossary.add_term(
        'revenue', 'Revenue', 'Total income from sales',
        owner='Finance', category='Metrics', tags=['financial', 'kpi']
    )
    assert 'revenue' in temp_glossary.terms
    assert temp_glossary.terms['revenue']['name'] == 'Revenue'


def test_link_asset_to_term(temp_glossary):
    temp_glossary.add_term('revenue', 'Revenue', 'Total income')
    temp_glossary.link_asset('revenue', 'catalog.schema.sales_fact')
    
    terms = temp_glossary.get_terms_for_asset('catalog.schema.sales_fact')
    assert len(terms) == 1
    assert terms[0]['name'] == 'Revenue'


def test_get_assets_for_term(temp_glossary):
    temp_glossary.add_term('customer', 'Customer', 'Business customer')
    temp_glossary.link_asset('customer', 'catalog.schema.customers')
    temp_glossary.link_asset('customer', 'catalog.schema.orders')
    
    assets = temp_glossary.get_assets_for_term('customer')
    assert len(assets) == 2
    assert 'catalog.schema.customers' in assets


def test_search_terms(temp_glossary):
    temp_glossary.add_term('revenue', 'Revenue', 'Total income', tags=['financial'])
    temp_glossary.add_term('profit', 'Profit', 'Net earnings', tags=['financial'])
    
    results = temp_glossary.search_terms('financial')
    assert len(results) == 2


def test_semantic_lineage_enrichment(temp_glossary):
    temp_glossary.add_term('revenue', 'Revenue', 'Total income')
    temp_glossary.link_asset('revenue', 'table_a')
    
    graph = nx.DiGraph()
    graph.add_edge('table_a', 'table_b')
    
    mapper = SemanticLineageMapper(temp_glossary)
    enriched = mapper.enrich_lineage_graph(graph)
    
    assert 'business_terms' in enriched.nodes['table_a']
    assert len(enriched.nodes['table_a']['business_terms']) == 1


def test_business_impact_analysis(temp_glossary):
    temp_glossary.add_term('revenue', 'Revenue', 'Total income')
    temp_glossary.link_asset('revenue', 'table_b')
    
    graph = nx.DiGraph()
    graph.add_edge('table_a', 'table_b')
    graph.add_edge('table_b', 'table_c')
    
    mapper = SemanticLineageMapper(temp_glossary)
    impact = mapper.get_business_impact('table_a', graph)
    
    assert impact['downstream_count'] == 2
    assert impact['affected_term_count'] == 1


def test_context_builder_integration(temp_glossary):
    builder = ContextBuilder(temp_glossary.storage_path)
    builder.get_glossary().add_term('customer', 'Customer', 'Business customer')
    builder.get_glossary().link_asset('customer', 'customers_table')
    
    context = builder.build_asset_context('customers_table')
    assert context['has_business_context'] is True
    assert len(context['business_terms']) == 1
