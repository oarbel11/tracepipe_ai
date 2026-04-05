import pytest
import tempfile
import os
from scripts.business_glossary import GlossaryManager
from scripts.semantic_lineage import SemanticLineageBuilder

@pytest.fixture
def glossary():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as f:
        db_path = f.name
    glossary = GlossaryManager(db_path)
    yield glossary
    os.unlink(db_path)

def test_add_term(glossary):
    term_id = glossary.add_term(
        term_name='Revenue',
        definition='Total sales amount',
        owner='Finance',
        category='Metrics'
    )
    assert term_id == 'revenue'

def test_link_term_to_asset(glossary):
    term_id = glossary.add_term('Customer', 'Customer information')
    glossary.link_term_to_asset(term_id, 'catalog.schema.customers', 'table')
    terms = glossary.get_terms_for_asset('catalog.schema.customers')
    assert len(terms) == 1
    assert terms[0]['term'] == 'Customer'

def test_get_assets_for_term(glossary):
    term_id = glossary.add_term('Order', 'Order transactions')
    glossary.link_term_to_asset(term_id, 'catalog.schema.orders', 'table')
    glossary.link_term_to_asset(term_id, 'catalog.schema.order_items', 'table')
    assets = glossary.get_assets_for_term(term_id)
    assert len(assets) == 2

def test_semantic_lineage_builder(glossary):
    builder = SemanticLineageBuilder(glossary)
    glossary.add_term('Revenue', 'Total revenue')
    glossary.link_term_to_asset('revenue', 'catalog.schema.sales')
    builder.add_technical_lineage('catalog.schema.orders', 'catalog.schema.sales')
    lineage = builder.get_semantic_lineage('catalog.schema.sales')
    assert lineage['asset'] == 'catalog.schema.sales'
    assert len(lineage['business_terms']) == 1
    assert lineage['business_terms'][0]['term'] == 'Revenue'

def test_business_impact_calculation(glossary):
    builder = SemanticLineageBuilder(glossary)
    glossary.add_term('Revenue', 'Total revenue', category='Finance')
    glossary.link_term_to_asset('revenue', 'catalog.schema.reports')
    builder.add_technical_lineage('catalog.schema.sales', 'catalog.schema.reports')
    lineage = builder.get_semantic_lineage('catalog.schema.sales')
    impact = lineage['business_impact']
    assert 'affected_assets' in impact
    assert 'affected_categories' in impact
