from typing import List, Dict, Any
from scripts.lineage_graph import LineageGraph, LineageNode


class ImpactAnalyzer:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def analyze_downstream_impact(self, node_id: str) -> Dict[str, Any]:
        downstream_nodes = self.graph.get_downstream(node_id)
        
        impact = {
            'affected_tables': [],
            'affected_views': [],
            'affected_bi_reports': [],
            'affected_etl_jobs': [],
            'total_count': len(downstream_nodes)
        }
        
        for node in downstream_nodes:
            if node.node_type == 'table':
                impact['affected_tables'].append(node.name)
            elif node.node_type == 'view':
                impact['affected_views'].append(node.name)
            elif node.node_type == 'bi_report':
                impact['affected_bi_reports'].append(node.name)
            elif node.node_type == 'etl_job':
                impact['affected_etl_jobs'].append(node.name)
        
        return impact

    def analyze_upstream_dependencies(self, node_id: str) -> Dict[str, Any]:
        upstream_nodes = self.graph.get_upstream(node_id)
        
        dependencies = {
            'source_tables': [],
            'source_files': [],
            'etl_jobs': [],
            'total_count': len(upstream_nodes)
        }
        
        for node in upstream_nodes:
            if node.node_type in ['table', 'view', 'external_table']:
                dependencies['source_tables'].append(node.name)
            elif node.node_type == 'file':
                dependencies['source_files'].append(node.name)
            elif node.node_type == 'etl_job':
                dependencies['etl_jobs'].append(node.name)
        
        return dependencies
