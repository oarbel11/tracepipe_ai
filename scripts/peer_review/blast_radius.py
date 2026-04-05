import networkx as nx
from typing import Dict, List, Set, Optional

class ImpactAnalysisMapper:
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph

    def get_downstream_impact(self, table_name: str, max_depth: int = None) -> Dict:
        if not self.graph.has_node(table_name):
            return {'table': table_name, 'downstream': [], 'depth': 0}
        
        downstream = nx.descendants(self.graph, table_name)
        
        impact_tree = self._build_impact_tree(table_name, max_depth)
        
        return {
            'table': table_name,
            'downstream': list(downstream),
            'count': len(downstream),
            'impact_tree': impact_tree,
            'depth': self._calculate_max_depth(table_name)
        }

    def _build_impact_tree(self, root: str, max_depth: Optional[int]) -> Dict:
        if max_depth is not None and max_depth <= 0:
            return {'name': root, 'children': []}
        
        children = []
        for successor in self.graph.successors(root):
            next_depth = max_depth - 1 if max_depth else None
            children.append(self._build_impact_tree(successor, next_depth))
        
        return {'name': root, 'children': children}

    def _calculate_max_depth(self, table_name: str) -> int:
        if not self.graph.has_node(table_name):
            return 0
        
        max_depth = 0
        for descendant in nx.descendants(self.graph, table_name):
            try:
                path_length = nx.shortest_path_length(self.graph, table_name, descendant)
                max_depth = max(max_depth, path_length)
            except nx.NetworkXNoPath:
                continue
        return max_depth

    def get_upstream_dependencies(self, table_name: str) -> List[str]:
        if not self.graph.has_node(table_name):
            return []
        return list(nx.ancestors(self.graph, table_name))

    def find_common_ancestors(self, table1: str, table2: str) -> List[str]:
        if not (self.graph.has_node(table1) and self.graph.has_node(table2)):
            return []
        
        ancestors1 = set(nx.ancestors(self.graph, table1))
        ancestors2 = set(nx.ancestors(self.graph, table2))
        return list(ancestors1.intersection(ancestors2))

    def get_blast_radius_with_quality(self, table_name: str,
                                     quality_scores: Dict[str, float]) -> Dict:
        impact = self.get_downstream_impact(table_name)
        
        enriched_downstream = []
        for table in impact['downstream']:
            enriched_downstream.append({
                'table': table,
                'quality_score': quality_scores.get(table, 100.0),
                'risk_level': self._assess_risk(quality_scores.get(table, 100.0))
            })
        
        return {
            'table': table_name,
            'downstream': enriched_downstream,
            'total_count': len(enriched_downstream),
            'high_risk_count': sum(1 for t in enriched_downstream if t['risk_level'] == 'high')
        }

    def _assess_risk(self, quality_score: float) -> str:
        if quality_score < 50:
            return 'high'
        elif quality_score < 80:
            return 'medium'
        return 'low'
