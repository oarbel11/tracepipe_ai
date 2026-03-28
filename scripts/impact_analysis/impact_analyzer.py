"""
Impact Analyzer

Analyzes downstream impact (blast radius) of changes to data assets.
"""

import networkx as nx
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass


@dataclass
class ImpactResult:
    """Result of impact analysis"""
    source_asset: str
    affected_tables: List[str]
    affected_views: List[str]
    affected_columns: List[str]
    impact_depth: int
    total_affected: int
    impact_paths: List[List[str]]


class ImpactAnalyzer:
    """Analyzes downstream impact of data asset changes"""
    
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
    
    def analyze_impact(self, asset_identifier: str, max_depth: int = 10) -> ImpactResult:
        """Analyze downstream impact of changes to an asset"""
        # Find the asset in the graph
        asset_node = self._find_asset(asset_identifier)
        if not asset_node:
            raise ValueError(f"Asset not found: {asset_identifier}")
        
        # Get all downstream nodes
        affected_nodes = self._get_downstream_nodes(asset_node, max_depth)
        
        # Categorize affected assets
        affected_tables = []
        affected_views = []
        affected_columns = []
        
        for node in affected_nodes:
            node_type = self.graph.nodes[node].get('type')
            if node_type == 'table':
                affected_tables.append(node)
            elif node_type == 'view':
                affected_views.append(node)
            elif node_type == 'column':
                affected_columns.append(node)
        
        # Find impact paths
        impact_paths = self._find_impact_paths(asset_node, affected_nodes, max_depth)
        
        # Calculate maximum depth
        max_path_depth = max([len(path) for path in impact_paths]) if impact_paths else 0
        
        return ImpactResult(
            source_asset=asset_node,
            affected_tables=affected_tables,
            affected_views=affected_views,
            affected_columns=affected_columns,
            impact_depth=max_path_depth,
            total_affected=len(affected_nodes),
            impact_paths=impact_paths
        )
    
    def simulate_change(self, asset_identifier: str, change_type: str = 'schema') -> Dict:
        """Simulate the impact of a specific change type"""
        impact = self.analyze_impact(asset_identifier)
        
        simulation = {
            'change_type': change_type,
            'source_asset': impact.source_asset,
            'risk_level': self._assess_risk_level(impact),
            'blast_radius': {
                'tables': len(impact.affected_tables),
                'views': len(impact.affected_views),
                'columns': len(impact.affected_columns),
                'total': impact.total_affected
            },
            'recommended_actions': self._generate_recommendations(impact, change_type),
            'affected_assets': {
                'tables': impact.affected_tables,
                'views': impact.affected_views,
                'columns': impact.affected_columns[:20]  # Limit to first 20
            }
        }
        
        return simulation
    
    def _find_asset(self, identifier: str) -> str:
        """Find asset in graph"""
        if identifier in self.graph:
            return identifier
        
        for node in self.graph.nodes():
            if node.endswith(f".{identifier}") or identifier in node:
                return node
        
        return None
    
    def _get_downstream_nodes(self, source_node: str, max_depth: int) -> Set[str]:
        """Get all downstream nodes up to max_depth"""
        affected = set()
        
        try:
            # Use BFS to find all reachable downstream nodes
            for target in self.graph.nodes():
                if target != source_node:
                    try:
                        path = nx.shortest_path(self.graph, source_node, target)
                        if len(path) <= max_depth + 1:  # +1 because path includes source
                            affected.add(target)
                    except nx.NetworkXNoPath:
                        continue
        except Exception:
            pass
        
        return affected
    
    def _find_impact_paths(self, source: str, targets: Set[str], max_depth: int) -> List[List[str]]:
        """Find paths from source to target nodes"""
        paths = []
        
        for target in targets:
            try:
                path = nx.shortest_path(self.graph, source, target)
                if len(path) <= max_depth + 1:
                    paths.append(path)
            except nx.NetworkXNoPath:
                continue
        
        # Return top 10 most important paths (shortest first)
        paths.sort(key=len)
        return paths[:10]
    
    def _assess_risk_level(self, impact: ImpactResult) -> str:
        """Assess risk level based on impact"""
        total = impact.total_affected
        
        if total == 0:
            return 'NONE'
        elif total <= 3:
            return 'LOW'
        elif total <= 10:
            return 'MEDIUM'
        elif total <= 25:
            return 'HIGH'
        else:
            return 'CRITICAL'
    
    def _generate_recommendations(self, impact: ImpactResult, change_type: str) -> List[str]:
        """Generate recommendations based on impact"""
        recommendations = []
        
        if impact.total_affected == 0:
            recommendations.append("No downstream dependencies detected. Change is safe.")
            return recommendations
        
        recommendations.append(f"Review all {impact.total_affected} affected assets before proceeding.")
        
        if impact.affected_views:
            recommendations.append(f"Update {len(impact.affected_views)} dependent views.")
        
        if impact.affected_tables:
            recommendations.append(f"Check {len(impact.affected_tables)} dependent tables for compatibility.")
        
        if change_type == 'schema':
            recommendations.append("Consider backward compatibility or versioning strategy.")
            recommendations.append("Test all downstream queries and transformations.")
        
        if impact.impact_depth > 5:
            recommendations.append("Deep dependency chain detected. Consider phased rollout.")
        
        return recommendations
