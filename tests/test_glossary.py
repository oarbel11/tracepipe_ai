"""Tests for Business Glossary & Catalog Enrichment."""
import os
import tempfile
import pytest
from scripts.glossary.manager import GlossaryManager
from scripts.glossary.enricher import CatalogEnricher


@pytest.fixture
def temp_storage():
    """Create temporary storage for tests."""
    temp_dir = tempfile.mkdtemp()
    storage_path = os.path.join(temp_dir, "test_glossary.json")
    yield storage_path
    if os.path.exists(storage_path):
        os.remove(storage_path)
    os.rmdir(temp_dir)


def test_glossary_manager_add_get(temp_storage):
    """Test adding and retrieving glossary terms."""
    manager = GlossaryManager(temp_storage)
    term = {
        'name': 'customer_id',
        'description': 'Unique customer identifier',
        'owner': 'data-team',
        'tags': ['pii', 'critical']
    }
    manager.add_term('catalog.schema.table.customer_id', term)
    retrieved = manager.get_term('catalog.schema.table.customer_id')
    assert retrieved is not None
    assert retrieved['name'] == 'customer_id'
    assert 'updated_at' in retrieved


def test_glossary_manager_update(temp_storage):
    """Test updating glossary terms."""
    manager = GlossaryManager(temp_storage)
    term = {'name': 'revenue', 'description': 'Total revenue'}
    manager.add_term('asset1', term)
    manager.update_term('asset1', {'owner': 'finance-team'})
    updated = manager.get_term('asset1')
    assert updated['owner'] == 'finance-team'
    assert updated['name'] == 'revenue'


def test_glossary_manager_delete(temp_storage):
    """Test deleting glossary terms."""
    manager = GlossaryManager(temp_storage)
    manager.add_term('asset1', {'name': 'test'})
    assert manager.delete_term('asset1') is True
    assert manager.get_term('asset1') is None
    assert manager.delete_term('nonexistent') is False


def test_catalog_enricher(temp_storage):
    """Test catalog enrichment."""
    manager = GlossaryManager(temp_storage)
    enricher = CatalogEnricher(manager)
    asset = {'id': 'table1', 'name': 'users'}
    metadata = {
        'description': 'User data table',
        'owner': 'data-team',
        'pii_status': True,
        'quality_score': 0.95
    }
    enriched = enricher.enrich_asset(asset, metadata)
    assert 'business_metadata' in enriched
    assert enriched['business_metadata']['pii_status'] is True
    assert enriched['business_metadata']['quality_score'] == 0.95


def test_enricher_operations(temp_storage):
    """Test enricher quality metrics and ownership."""
    manager = GlossaryManager(temp_storage)
    enricher = CatalogEnricher(manager)
    manager.add_term('asset1', {'name': 'test_asset'})
    assert enricher.set_ownership('asset1', 'john@example.com') is True
    assert enricher.add_tags('asset1', ['important', 'reviewed']) is True
    term = manager.get_term('asset1')
    assert term['owner'] == 'john@example.com'
    assert 'important' in term['tags']
