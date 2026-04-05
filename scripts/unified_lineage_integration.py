import json
from typing import Dict, Any, List
from scripts.workspace_connector import WorkspaceConnector
from scripts.workspace_lineage_aggregator import WorkspaceLineageAggregator
from scripts.context_enricher import ContextEnricher

class UnifiedLineageIntegration:
    """Integrates unified workspace lineage into the system."""
    
    def __init__(self, workspace_configs: List[Dict[str, str]]):
        self.connector = WorkspaceConnector(workspace_configs)
        self.aggregator = WorkspaceLineageAggregator()
        self.enricher = ContextEnricher()
    
    def build_unified_lineage(self) -> Dict[str, Any]:
        """Build unified lineage across all workspaces."""
        workspace_data = self.connector.fetch_all_objects()
        self.aggregator.add_workspace_objects(workspace_data)
        
        for workspace_id, objects in workspace_data.items():
            for obj_type, obj_list in objects.items():
                enriched = self.enricher.enrich_objects(obj_list)
                for obj in enriched:
                    obj_id = obj["id"]
                    if obj_id in self.aggregator.all_objects:
                        self.aggregator.all_objects[obj_id] = obj
        
        return self.aggregator.get_all_lineage()
    
    def get_cross_workspace_impact(self, object_id: str) -> Dict[str, Any]:
        """Analyze cross-workspace impact for an object."""
        lineage = self.aggregator.get_object_lineage(object_id)
        if not lineage:
            return {"impact": "none", "affected_workspaces": []}
        
        affected_workspaces = set()
        obj = self.aggregator.all_objects.get(object_id, {})
        origin_workspace = obj.get("workspace_id")
        
        for upstream_id in lineage.get("upstream", []):
            upstream_obj = self.aggregator.all_objects.get(upstream_id, {})
            workspace = upstream_obj.get("workspace_id")
            if workspace and workspace != origin_workspace:
                affected_workspaces.add(workspace)
        
        for downstream_id in lineage.get("downstream", []):
            downstream_obj = self.aggregator.all_objects.get(downstream_id, {})
            workspace = downstream_obj.get("workspace_id")
            if workspace and workspace != origin_workspace:
                affected_workspaces.add(workspace)
        
        return {
            "impact": "cross-workspace" if affected_workspaces else "single-workspace",
            "affected_workspaces": list(affected_workspaces),
            "upstream_count": len(lineage.get("upstream", [])),
            "downstream_count": len(lineage.get("downstream", []))
        }
