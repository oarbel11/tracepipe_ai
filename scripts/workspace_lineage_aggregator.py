import json
from typing import Dict, List, Any, Set
from collections import defaultdict

class WorkspaceLineageAggregator:
    """Aggregates lineage across multiple workspaces."""
    
    def __init__(self):
        self.lineage_graph = defaultdict(dict)
        self.all_objects = {}
    
    def add_workspace_objects(self, workspace_data: Dict[str, Any]):
        """Add objects from workspaces to the aggregator."""
        for workspace_id, objects in workspace_data.items():
            for obj_type, obj_list in objects.items():
                for obj in obj_list:
                    obj_id = obj["id"]
                    self.all_objects[obj_id] = obj
                    self.lineage_graph[obj_id] = {
                        "upstream": [],
                        "downstream": [],
                        "metadata": obj
                    }
    
    def add_lineage_edge(self, source_id: str, target_id: str):
        """Add a lineage relationship between objects."""
        if source_id in self.lineage_graph:
            self.lineage_graph[source_id]["downstream"].append(target_id)
        if target_id in self.lineage_graph:
            self.lineage_graph[target_id]["upstream"].append(source_id)
    
    def get_object_lineage(self, object_id: str) -> Dict[str, Any]:
        """Get complete lineage for an object."""
        if object_id not in self.lineage_graph:
            return {}
        return self.lineage_graph[object_id]
    
    def get_cross_workspace_lineage(self) -> Dict[str, Any]:
        """Get lineage spanning multiple workspaces."""
        cross_workspace = {}
        for obj_id, lineage in self.lineage_graph.items():
            obj_workspace = self.all_objects[obj_id].get("workspace_id")
            has_cross_workspace = False
            for upstream_id in lineage["upstream"]:
                if self.all_objects[upstream_id].get("workspace_id") != obj_workspace:
                    has_cross_workspace = True
                    break
            if has_cross_workspace:
                cross_workspace[obj_id] = lineage
        return cross_workspace
    
    def get_all_lineage(self) -> Dict[str, Any]:
        """Get complete lineage graph."""
        return dict(self.lineage_graph)
