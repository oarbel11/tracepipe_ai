import pytest
import os
import tempfile
from scripts.glossary import Term, Ownership, Tag, GlossaryManager, CatalogEnricher


@pytest.fixture
def temp_storage():
    fd, path = tempfile.mkstemp(suffix='.json')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


def test_term_creation():
    term = Term(
        name='customer_id',
        definition='Unique identifier for customers',
        catalog_path='catalog.schema.customer_id',
        pii_status=True
    )
    assert term.name == 'customer_id'
    assert term.pii_status is True


def test_glossary_manager_add_get(temp_storage):
    manager = GlossaryManager(temp_storage)
    term = Term(
        name='order_date',
        definition='Date when order was placed',
        catalog_path='catalog.orders.order_date'
    )
    manager.add_term(term)
    retrieved = manager.get_term('catalog.orders.order_date')
    assert retrieved is not None
    assert retrieved.name == 'order_date'


def test_glossary_manager_update(temp_storage):
    manager = GlossaryManager(temp_storage)
    term = Term(
        name='price',
        definition='Product price',
        catalog_path='catalog.products.price'
    )
    manager.add_term(term)
    updated = manager.update_term('catalog.products.price', definition='Updated price')
    assert updated.definition == 'Updated price'


def test_glossary_manager_delete(temp_storage):
    manager = GlossaryManager(temp_storage)
    term = Term(
        name='temp',
        definition='Temporary field',
        catalog_path='catalog.temp.field'
    )
    manager.add_term(term)
    assert manager.delete_term('catalog.temp.field') is True
    assert manager.get_term('catalog.temp.field') is None


def test_catalog_enricher(temp_storage):
    manager = GlossaryManager(temp_storage)
    enricher = CatalogEnricher(manager)
    term = Term(
        name='customer_email',
        definition='Customer email address',
        catalog_path='catalog.users.email',
        ownership=Ownership('data-team', 'engineering'),
        pii_status=True
    )
    manager.add_term(term)
    asset = {'name': 'email', 'catalog_path': 'catalog.users.email'}
    enriched = enricher.enrich_asset('catalog.users.email', asset)
    assert 'business_metadata' in enriched
    assert enriched['business_metadata']['pii_status'] is True
