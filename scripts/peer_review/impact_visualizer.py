import networkx as nx
from typing import Dict, List, Any, Optional, Tuple
import json

class ImpactVisualizer:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.impact_data = {}

    def build_impact_graph(self, root_object: str, impacts: List[Any]) -> nx.DiGraph:
        self.graph.clear()
        self.graph.add_node(root_object, node_type='source', severity='none')
        
        for impact in impacts:
            affected = getattr(impact, 'affected_object', None)
            if affected:
                self.graph.add_node(
                    affected,
                    node_type=getattr(impact, 'object_type', 'unknown'),
                    severity=getattr(impact, 'severity', 'low')
                )
                self.graph.add_edge(root_object, affected, change_type=getattr(impact, 'change_type', 'unknown'))
                
                details = getattr(impact, 'details', {})
                if isinstance(details, dict) and 'downstream' in details:
                    self._add_downstream_nodes(affected, details['downstream'])
        
        return self.graph

    def _add_downstream_nodes(self, parent: str, downstream: Dict[str, List[str]]):
        for obj_type, objects in downstream.items():
            for obj in objects:
                self.graph.add_node(obj, node_type=obj_type, severity='inherited')
                self.graph.add_edge(parent, obj, change_type='dependency')

    def get_visualization_data(self) -> Dict[str, Any]:
        nodes = []
        edges = []
        
        for node, attrs in self.graph.nodes(data=True):
            nodes.append({
                'id': node,
                'type': attrs.get('node_type', 'unknown'),
                'severity': attrs.get('severity', 'none')
            })
        
        for source, target, attrs in self.graph.edges(data=True):
            edges.append({
                'source': source,
                'target': target,
                'type': attrs.get('change_type', 'unknown')
            })
        
        return {'nodes': nodes, 'edges': edges}

    def calculate_blast_radius(self, root_object: str) -> Dict[str, Any]:
        if root_object not in self.graph:
            return {'error': 'Object not found'}
        
        descendants = nx.descendants(self.graph, root_object)
        
        severity_counts = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        type_counts = {}
        
        for desc in descendants:
            severity = self.graph.nodes[desc].get('severity', 'low')
            if severity in severity_counts:
                severity_counts[severity] += 1
            
            obj_type = self.graph.nodes[desc].get('node_type', 'unknown')
            type_counts[obj_type] = type_counts.get(obj_type, 0) + 1
        
        return {
            'total_affected': len(descendants),
            'by_severity': severity_counts,
            'by_type': type_counts,
            'critical_path_length': self._max_path_length(root_object)
        }

    def _max_path_length(self, root: str) -> int:
        try:
            return max(len(nx.shortest_path(self.graph, root, node)) - 1 
                      for node in nx.descendants(self.graph, root))
        except:
            return 0
