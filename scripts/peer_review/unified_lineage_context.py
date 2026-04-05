from typing import Dict, List, Any, Optional
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.unity_catalog_connector import UnityCatalogConnector
from scripts.workspace_lineage_aggregator import WorkspaceLineageAggregator


class UnifiedLineageContext:
    def __init__(self, config_path: str = "config/config.yml"):
        self.connector = UnityCatalogConnector(config_path)
        self.aggregator = WorkspaceLineageAggregator()
        self._initialized = False

    def initialize(self):
        workspace_objects = self.connector.fetch_all_workspaces()
        self.aggregator.add_workspace_objects(workspace_objects)
        self._infer_lineage_relationships(workspace_objects)
        self._initialized = True

    def _infer_lineage_relationships(self, workspace_objects: Dict[str, List[Dict[str, Any]]]):
        for ws_id, objects in workspace_objects.items():
            for obj in objects:
                if obj['type'] == 'jobs':
                    self._link_job_dependencies(ws_id, obj)

    def _link_job_dependencies(self, workspace_id: str, job_obj: Dict[str, Any]):
        job_id = self.aggregator._generate_object_id(workspace_id, job_obj)
        data = job_obj['data']
        
        if 'notebook_path' in data:
            notebook_id = f"{workspace_id}:notebook:{data['notebook_path']}"
            if notebook_id in self.aggregator.object_metadata:
                self.aggregator.add_lineage_edge(job_id, notebook_id, "executes")

    def get_enriched_context(self, object_identifier: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        if not self._initialized:
            self.initialize()
        
        if workspace_id:
            full_id = f"{workspace_id}:{object_identifier}"
        else:
            full_id = object_identifier
        
        lineage = self.aggregator.get_unified_lineage(full_id)
        
        if 'error' not in lineage:
            lineage['blast_radius'] = self._calculate_blast_radius(full_id)
            lineage['impact_score'] = self._calculate_impact_score(full_id)
        
        return lineage

    def _calculate_blast_radius(self, object_id: str) -> int:
        try:
            downstream = list(self.aggregator.lineage_graph.successors(object_id))
            return len(downstream)
        except:
            return 0

    def _calculate_impact_score(self, object_id: str) -> float:
        blast_radius = self._calculate_blast_radius(object_id)
        cross_workspace = self.aggregator._detect_cross_workspace(object_id)
        score = blast_radius * 1.0
        if cross_workspace:
            score *= 1.5
        return round(score, 2)

    def get_workspace_summary(self) -> Dict[str, Any]:
        if not self._initialized:
            self.initialize()
        
        summary = {}
        for ws_id, objects in self.aggregator.workspace_index.items():
            summary[ws_id] = {
                'total_objects': len(objects),
                'cross_workspace_flows': len([e for e in self.aggregator.get_all_cross_workspace_flows() 
                                             if e[0].startswith(ws_id) or e[1].startswith(ws_id)])
            }
        return summary

    def close(self):
        self.connector.close_all()
