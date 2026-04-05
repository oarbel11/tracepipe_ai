import pytest
import tempfile
import os
from scripts.glossary import Term, GlossaryManager, CatalogEnricher

@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)

@pytest.fixture
def glossary_mgr(temp_db):
    return GlossaryManager(db_path=temp_db)

@pytest.fixture
def enricher(glossary_mgr):
    return CatalogEnricher(glossary_mgr)

def test_add_and_get_term(glossary_mgr):
    term = Term(
        name='customer_email',
        definition='Email address of customer',
        owner='data-team',
        tags=['PII', 'contact'],
        is_pii=True,
        quality_score=98.5
    )
    glossary_mgr.add_term(term)
    retrieved = glossary_mgr.get_term('customer_email')
    assert retrieved is not None
    assert retrieved.name == 'customer_email'
    assert retrieved.is_pii is True
    assert retrieved.quality_score == 98.5
    assert 'PII' in retrieved.tags

def test_list_terms_by_tag(glossary_mgr):
    term1 = Term(name='t1', definition='d1', owner='o1', tags=['PII'])
    term2 = Term(name='t2', definition='d2', owner='o2', tags=['critical'])
    glossary_mgr.add_term(term1)
    glossary_mgr.add_term(term2)
    pii_terms = glossary_mgr.list_terms(tag='PII')
    assert len(pii_terms) == 1
    assert pii_terms[0].name == 't1'

def test_enrich_column(enricher, glossary_mgr):
    term = Term(name='user_id', definition='User identifier',
                owner='eng', tags=['key'])
    glossary_mgr.add_term(term)
    enricher.enrich_column('catalog1', 'schema1', 'users', 'id', 'user_id')
    retrieved = enricher.get_enrichment('catalog1', 'schema1', 'users', 'id')
    assert retrieved is not None
    assert retrieved.name == 'user_id'

def test_find_pii_columns(enricher, glossary_mgr):
    term = Term(name='email', definition='Email', owner='data',
                tags=['PII'], is_pii=True)
    glossary_mgr.add_term(term)
    enricher.enrich_column('cat', 'sch', 'users', 'email', 'email')
    pii_cols = enricher.find_pii_columns('cat', 'sch')
    assert len(pii_cols) == 1
    assert pii_cols[0]['column'] == 'email'

def test_table_enrichments(enricher, glossary_mgr):
    t1 = Term(name='id', definition='ID', owner='eng')
    t2 = Term(name='name', definition='Name', owner='eng')
    glossary_mgr.add_term(t1)
    glossary_mgr.add_term(t2)
    enricher.enrich_column('c', 's', 'tbl', 'id', 'id')
    enricher.enrich_column('c', 's', 'tbl', 'name', 'name')
    enrichments = enricher.get_table_enrichments('c', 's', 'tbl')
    assert len(enrichments) == 2
    assert 'id' in enrichments
    assert 'name' in enrichments