import pytest
import networkx as nx
from pathlib import Path
import tempfile
import os
from scripts.metadata_store import MetadataStore
from scripts.lineage_enricher import LineageEnricher
from scripts.context_viewer import ContextViewer


@pytest.fixture
def temp_metadata_store():
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = os.path.join(tmpdir, "metadata.json")
        yield MetadataStore(store_path)


@pytest.fixture
def sample_lineage_graph():
    g = nx.DiGraph()
    g.add_node("table.users", type="table")
    g.add_node("table.orders", type="table")
    g.add_node("view.user_orders", type="view")
    g.add_edge("table.users", "view.user_orders")
    g.add_edge("table.orders", "view.user_orders")
    return g


def test_metadata_store_add_glossary(temp_metadata_store):
    store = temp_metadata_store
    store.add_glossary_term("table.users", "Customer", 
                           "A person who purchases products", "business")
    metadata = store.get_metadata("table.users")
    assert len(metadata["glossary"]) == 1
    assert metadata["glossary"][0]["term"] == "Customer"


def test_metadata_store_add_owner(temp_metadata_store):
    store = temp_metadata_store
    store.add_owner("table.orders", "John Doe", "Data Steward", "john@example.com")
    metadata = store.get_metadata("table.orders")
    assert len(metadata["owners"]) == 1
    assert metadata["owners"][0]["name"] == "John Doe"


def test_metadata_store_add_quality_rule(temp_metadata_store):
    store = temp_metadata_store
    store.add_quality_rule("table.users", "completeness", 
                          "Email must not be null", 0.95)
    metadata = store.get_metadata("table.users")
    assert len(metadata["quality_rules"]) == 1
    assert metadata["quality_rules"][0]["threshold"] == 0.95


def test_metadata_store_search(temp_metadata_store):
    store = temp_metadata_store
    store.add_glossary_term("table.users", "Customer", "A purchaser", "business")
    results = store.search_by_term("customer")
    assert len(results) == 1
    assert results[0]["entity_id"] == "table.users"


def test_lineage_enricher_enrich_graph(temp_metadata_store, sample_lineage_graph):
    store = temp_metadata_store
    store.add_glossary_term("table.users", "Customer", "User data", "business")
    enricher = LineageEnricher(store)
    enriched = enricher.enrich_graph(sample_lineage_graph)
    assert "metadata" in enriched.nodes["table.users"]
    assert enriched.nodes["table.users"]["has_context"] is True


def test_lineage_enricher_context_summary(temp_metadata_store, sample_lineage_graph):
    store = temp_metadata_store
    store.add_owner("table.users", "Alice", "Owner", "alice@example.com")
    enricher = LineageEnricher(store)
    summary = enricher.get_context_summary(sample_lineage_graph)
    assert summary["total_nodes"] == 3
    assert summary["nodes_with_context"] == 1


def test_context_viewer_display(temp_metadata_store, sample_lineage_graph):
    store = temp_metadata_store
    store.add_glossary_term("table.users", "Customer", "User data", "business")
    enricher = LineageEnricher(store)
    viewer = ContextViewer(enricher)
    output = viewer.display_node_context(sample_lineage_graph, "table.users")
    assert "Customer" in output
    assert "User data" in output
