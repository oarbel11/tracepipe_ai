import pytest
from tracepipe_ai.business_context import (
    MetadataStore, LineageEnricher, ContextViewer
)


def test_metadata_store_terms():
    store = MetadataStore()
    term_id = store.add_term('t1', 'Revenue', 'Total company revenue', 'Finance')
    assert term_id == 't1'
    term = store.get_term('t1')
    assert term['name'] == 'Revenue'
    assert term['definition'] == 'Total company revenue'
    assert 't1' in store.list_terms()


def test_metadata_store_owners():
    store = MetadataStore()
    owner_id = store.add_owner('e1', 'John Doe', 'john@example.com')
    assert owner_id == 'e1'
    owner = store.get_owner('e1')
    assert owner['name'] == 'John Doe'
    assert owner['email'] == 'john@example.com'


def test_metadata_store_quality_rules():
    store = MetadataStore()
    rule_id = store.add_quality_rule('r1', 'e1', 'not_null', 'field must not be null')
    assert rule_id == 'r1'
    rules = store.get_quality_rules('e1')
    assert len(rules) == 1
    assert rules[0]['type'] == 'not_null'


def test_lineage_enricher():
    store = MetadataStore()
    store.add_term('t1', 'Revenue', 'Total revenue', 'Finance')
    store.add_owner('e1', 'Jane Smith', 'jane@example.com')
    
    enricher = LineageEnricher(store)
    enriched = enricher.enrich_node('node1', term_ids=['t1'], owner_id='e1')
    
    assert len(enriched['terms']) == 1
    assert enriched['terms'][0]['term']['name'] == 'Revenue'
    assert enriched['owner']['name'] == 'Jane Smith'


def test_context_viewer():
    store = MetadataStore()
    store.add_term('t1', 'Revenue', 'Total revenue', 'Finance')
    store.add_owner('e1', 'Jane Smith', 'jane@example.com')
    
    enricher = LineageEnricher(store)
    enricher.enrich_node('node1', term_ids=['t1'], owner_id='e1')
    
    viewer = ContextViewer(enricher)
    context = viewer.get_node_context('node1')
    
    assert context['node_id'] == 'node1'
    assert len(context['business_terms']) == 1
    assert context['business_terms'][0]['name'] == 'Revenue'
    assert context['owner']['name'] == 'Jane Smith'


def test_lineage_graph_enrichment():
    store = MetadataStore()
    store.add_term('t1', 'Revenue', 'Total revenue', 'Finance')
    enricher = LineageEnricher(store)
    enricher.enrich_node('node1', term_ids=['t1'])
    
    viewer = ContextViewer(enricher)
    lineage = {'nodes': [{'id': 'node1', 'name': 'Table1'}], 'edges': []}
    enriched = viewer.get_lineage_with_context(lineage)
    
    assert len(enriched['nodes']) == 1
    assert 'context' in enriched['nodes'][0]


def test_search_by_term():
    store = MetadataStore()
    store.add_term('t1', 'Revenue', 'Total revenue', 'Finance')
    enricher = LineageEnricher(store)
    enricher.enrich_node('node1', term_ids=['t1'])
    enricher.enrich_node('node2', term_ids=['t1'])
    
    viewer = ContextViewer(enricher)
    results = viewer.search_by_term('Revenue')
    
    assert len(results) == 2
    assert 'node1' in results
    assert 'node2' in results
