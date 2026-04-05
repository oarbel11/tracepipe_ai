import pytest
import json
import sys
import os

from scripts.workspace_connector import WorkspaceConnector
from scripts.workspace_lineage_aggregator import WorkspaceLineageAggregator
from scripts.context_enricher import ContextEnricher
from scripts.unified_lineage_integration import UnifiedLineageIntegration

def test_workspace_connector():
    """Test workspace connector fetches objects."""
    configs = [{"workspace_id": "ws1"}, {"workspace_id": "ws2"}]
    connector = WorkspaceConnector(configs)
    all_objects = connector.fetch_all_objects()
    
    assert "ws1" in all_objects
    assert "ws2" in all_objects
    assert "notebooks" in all_objects["ws1"]
    assert len(all_objects["ws1"]["notebooks"]) > 0

def test_lineage_aggregator():
    """Test lineage aggregator builds graph."""
    aggregator = WorkspaceLineageAggregator()
    workspace_data = {
        "ws1": {
            "notebooks": [{"id": "nb1", "name": "Test", "type": "notebook", "workspace_id": "ws1"}]
        }
    }
    aggregator.add_workspace_objects(workspace_data)
    lineage = aggregator.get_object_lineage("nb1")
    
    assert lineage is not None
    assert "upstream" in lineage
    assert "downstream" in lineage

def test_context_enricher():
    """Test context enricher adds metadata."""
    enricher = ContextEnricher()
    notebook = {"id": "nb1", "name": "Test", "type": "notebook", "language": "python"}
    enriched = enricher.enrich_notebook(notebook)
    
    assert "enriched_metadata" in enriched
    assert enriched["enriched_metadata"]["object_type"] == "notebook"

def test_unified_lineage_integration():
    """Test unified lineage integration."""
    configs = [{"workspace_id": "ws1"}, {"workspace_id": "ws2"}]
    integration = UnifiedLineageIntegration(configs)
    unified_lineage = integration.build_unified_lineage()
    
    assert unified_lineage is not None
    assert len(unified_lineage) > 0

def test_cross_workspace_impact():
    """Test cross-workspace impact analysis."""
    configs = [{"workspace_id": "ws1"}]
    integration = UnifiedLineageIntegration(configs)
    integration.build_unified_lineage()
    
    impact = integration.get_cross_workspace_impact("notebook_ws1_1")
    assert "impact" in impact
    assert "affected_workspaces" in impact
