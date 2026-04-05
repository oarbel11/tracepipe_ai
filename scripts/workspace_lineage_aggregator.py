from typing import Dict, List, Any, Set, Tuple
import networkx as nx
from collections import defaultdict


class WorkspaceLineageAggregator:
    def __init__(self):
        self.lineage_graph = nx.DiGraph()
        self.object_metadata = {}
        self.workspace_index = defaultdict(list)

    def add_workspace_objects(self, workspace_objects: Dict[str, List[Dict[str, Any]]]):
        for ws_id, objects in workspace_objects.items():
            for obj in objects:
                obj_id = self._generate_object_id(ws_id, obj)
                self.object_metadata[obj_id] = {
                    'workspace_id': ws_id,
                    'type': obj['type'],
                    'data': obj['data'],
                    'discovered_at': obj.get('discovered_at')
                }
                self.workspace_index[ws_id].append(obj_id)
                self.lineage_graph.add_node(obj_id, **self.object_metadata[obj_id])

    def _generate_object_id(self, workspace_id: str, obj: Dict[str, Any]) -> str:
        obj_type = obj['type']
        data = obj['data']
        if obj_type == 'notebooks':
            return f"{workspace_id}:notebook:{data.get('object_id', data.get('path'))}"
        elif obj_type == 'dashboards':
            return f"{workspace_id}:dashboard:{data.get('dashboard_id')}"
        elif obj_type == 'jobs':
            return f"{workspace_id}:job:{data.get('job_id')}"
        return f"{workspace_id}:{obj_type}:unknown"

    def add_lineage_edge(self, source_id: str, target_id: str, edge_type: str = "uses"):
        self.lineage_graph.add_edge(source_id, target_id, type=edge_type)

    def get_unified_lineage(self, object_id: str) -> Dict[str, Any]:
        if object_id not in self.lineage_graph:
            return {'error': 'Object not found'}
        
        upstream = list(self.lineage_graph.predecessors(object_id))
        downstream = list(self.lineage_graph.successors(object_id))
        
        return {
            'object_id': object_id,
            'metadata': self.object_metadata.get(object_id, {}),
            'upstream': [self._get_object_summary(oid) for oid in upstream],
            'downstream': [self._get_object_summary(oid) for oid in downstream],
            'cross_workspace': self._detect_cross_workspace(object_id)
        }

    def _get_object_summary(self, object_id: str) -> Dict[str, Any]:
        meta = self.object_metadata.get(object_id, {})
        return {
            'object_id': object_id,
            'workspace_id': meta.get('workspace_id'),
            'type': meta.get('type'),
            'name': self._extract_name(meta)
        }

    def _extract_name(self, metadata: Dict[str, Any]) -> str:
        data = metadata.get('data', {})
        return data.get('name') or data.get('path') or 'Unknown'

    def _detect_cross_workspace(self, object_id: str) -> bool:
        workspace_id = self.object_metadata.get(object_id, {}).get('workspace_id')
        related = set(self.lineage_graph.predecessors(object_id)) | set(self.lineage_graph.successors(object_id))
        return any(self.object_metadata.get(r, {}).get('workspace_id') != workspace_id for r in related)

    def get_all_cross_workspace_flows(self) -> List[Tuple[str, str]]:
        cross_workspace_edges = []
        for source, target in self.lineage_graph.edges():
            source_ws = self.object_metadata.get(source, {}).get('workspace_id')
            target_ws = self.object_metadata.get(target, {}).get('workspace_id')
            if source_ws and target_ws and source_ws != target_ws:
                cross_workspace_edges.append((source, target))
        return cross_workspace_edges
