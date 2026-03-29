import networkx as nx
from typing import List, Dict, Set
from .quality_monitor import QualityIssue, DataQualityMonitor
from ..peer_review.blast_radius import ImpactAnalysisMapper


class LineageQualityIntegrator:
    def __init__(self, quality_monitor: DataQualityMonitor):
        self.quality_monitor = quality_monitor
        self.impact_mapper = ImpactAnalysisMapper()

    def build_quality_graph(self, lineage_graph: nx.DiGraph, issues: List[QualityIssue]) -> nx.DiGraph:
        enhanced_graph = lineage_graph.copy()
        
        for node in enhanced_graph.nodes():
            enhanced_graph.nodes[node]['quality_status'] = 'healthy'
            enhanced_graph.nodes[node]['quality_issues'] = []
        
        for issue in issues:
            if issue.asset_name in enhanced_graph.nodes():
                enhanced_graph.nodes[issue.asset_name]['quality_status'] = 'unhealthy'
                enhanced_graph.nodes[issue.asset_name]['quality_issues'].append({
                    'type': issue.issue_type,
                    'description': issue.description,
                    'severity': issue.severity
                })
                
                downstream = self._get_downstream_nodes(enhanced_graph, issue.asset_name)
                issue.affected_downstream = list(downstream)
                
                for downstream_node in downstream:
                    if enhanced_graph.nodes[downstream_node]['quality_status'] == 'healthy':
                        enhanced_graph.nodes[downstream_node]['quality_status'] = 'at_risk'
                    enhanced_graph.nodes[downstream_node]['quality_issues'].append({
                        'type': 'upstream_issue',
                        'description': f"Upstream issue in {issue.asset_name}",
                        'severity': 'low'
                    })
        
        return enhanced_graph

    def _get_downstream_nodes(self, graph: nx.DiGraph, source_node: str) -> Set[str]:
        try:
            descendants = nx.descendants(graph, source_node)
            return descendants
        except nx.NetworkXError:
            return set()

    def get_quality_summary(self, graph: nx.DiGraph) -> Dict[str, int]:
        summary = {
            'healthy': 0,
            'at_risk': 0,
            'unhealthy': 0,
            'total_issues': 0
        }
        
        for node in graph.nodes():
            status = graph.nodes[node].get('quality_status', 'healthy')
            summary[status] = summary.get(status, 0) + 1
            issues = graph.nodes[node].get('quality_issues', [])
            summary['total_issues'] += len(issues)
        
        return summary

    def get_critical_path(self, graph: nx.DiGraph) -> List[str]:
        critical_nodes = []
        for node in graph.nodes():
            if graph.nodes[node].get('quality_status') == 'unhealthy':
                downstream = self._get_downstream_nodes(graph, node)
                if len(downstream) > 0:
                    critical_nodes.append(node)
        
        return sorted(critical_nodes, key=lambda n: len(self._get_downstream_nodes(graph, n)), reverse=True)

    def annotate_lineage(self, lineage_graph: nx.DiGraph, table_list: List[str]) -> nx.DiGraph:
        issues = self.quality_monitor.get_all_issues(table_list)
        return self.build_quality_graph(lineage_graph, issues)
