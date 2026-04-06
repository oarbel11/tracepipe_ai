import pytest
from scripts.peer_review.business_glossary import BusinessGlossary, BusinessTerm
from scripts.peer_review.semantic_mapper import SemanticMapper, SemanticLink

def test_create_business_term():
    term = BusinessTerm(
        term_id="customer_id",
        name="Customer ID",
        definition="Unique identifier for customers",
        synonyms=["cust_id", "customer_number"]
    )
    assert term.term_id == "customer_id"
    assert term.name == "Customer ID"
    assert "cust_id" in term.synonyms

def test_business_glossary_add_and_search():
    glossary = BusinessGlossary()
    term = BusinessTerm(
        term_id="revenue",
        name="Annual Revenue",
        definition="Total income from sales"
    )
    glossary.add_term(term)
    
    assert glossary.get_term("revenue") == term
    results = glossary.search_terms("revenue")
    assert len(results) == 1
    assert results[0].term_id == "revenue"

def test_semantic_mapper_link_term_to_asset():
    mapper = SemanticMapper()
    mapper.add_business_term(
        term_id="revenue",
        name="Revenue",
        definition="Total sales income"
    )
    mapper.link_term_to_asset(
        term_id="revenue",
        asset_type="column",
        asset_id="companies_data.corporate.companies.revenue"
    )
    
    terms = mapper.get_terms_for_asset("companies_data.corporate.companies.revenue")
    assert len(terms) == 1
    assert terms[0].term_id == "revenue"

def test_semantic_mapper_search_by_business_term():
    mapper = SemanticMapper()
    mapper.add_business_term(
        term_id="customer",
        name="Customer",
        definition="Individual or organization that purchases",
        synonyms=["client", "buyer"]
    )
    mapper.link_term_to_asset(
        term_id="customer",
        asset_type="table",
        asset_id="sales.customers"
    )
    
    results = mapper.search_by_business_term("client")
    assert len(results) == 1
    assert results[0]["asset_id"] == "sales.customers"

def test_get_assets_for_term():
    mapper = SemanticMapper()
    mapper.add_business_term(
        term_id="order",
        name="Order",
        definition="Purchase transaction"
    )
    mapper.link_term_to_asset("order", "table", "sales.orders")
    mapper.link_term_to_asset("order", "table", "warehouse.shipments")
    
    assets = mapper.get_assets_for_term("order")
    assert len(assets) == 2
    asset_ids = [a["asset_id"] for a in assets]
    assert "sales.orders" in asset_ids
    assert "warehouse.shipments" in asset_ids

def test_glossary_export_import():
    glossary = BusinessGlossary()
    term = BusinessTerm(
        term_id="profit",
        name="Profit",
        definition="Revenue minus costs",
        category="finance"
    )
    glossary.add_term(term)
    
    exported = glossary.export_glossary()
    new_glossary = BusinessGlossary()
    new_glossary.import_glossary(exported)
    
    imported_term = new_glossary.get_term("profit")
    assert imported_term.name == "Profit"
    assert imported_term.category == "finance"
