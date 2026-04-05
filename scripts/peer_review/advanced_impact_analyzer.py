import networkx as nx
from typing import Dict, List, Set, Optional, Any
from .blast_radius import ImpactAnalysisMapper
from .data_quality_integration import DataQualityIntegration

class AdvancedImpactAnalyzer:
    def __init__(self, lineage_graph: nx.DiGraph, quality_integration: DataQualityIntegration):
        self.graph = lineage_graph
        self.quality = quality_integration
        self.impact_mapper = ImpactAnalysisMapper(lineage_graph)

    def analyze_change_impact(self, changed_table: str, change_type: str = 'schema') -> Dict[str, Any]:
        downstream = list(nx.descendants(self.graph, changed_table))
        upstream = list(nx.ancestors(self.graph, changed_table))
        
        impact_score = self._calculate_impact_score(changed_table, downstream)
        quality_risks = self._assess_quality_risks(changed_table, downstream)
        
        return {
            'changed_table': changed_table,
            'change_type': change_type,
            'downstream_count': len(downstream),
            'upstream_count': len(upstream),
            'impact_score': impact_score,
            'downstream_tables': downstream,
            'quality_risks': quality_risks,
            'critical_paths': self._find_critical_paths(changed_table, downstream)
        }

    def _calculate_impact_score(self, source: str, downstream: List[str]) -> float:
        base_score = min(len(downstream) * 10, 100)
        quality_penalty = 0
        
        for table in [source] + downstream[:5]:
            if self.graph.has_node(table):
                quality_info = self.quality.get_table_quality_score(table)
                if quality_info['status'] == 'critical':
                    quality_penalty += 20
                elif quality_info['status'] == 'degraded':
                    quality_penalty += 10
        
        return min(base_score + quality_penalty, 100)

    def _assess_quality_risks(self, source: str, downstream: List[str]) -> List[Dict]:
        risks = []
        for table in [source] + downstream:
            if self.graph.has_node(table):
                quality_info = self.quality.get_table_quality_score(table)
                signals = self.quality.get_recent_signals(table, hours=24)
                
                if quality_info['status'] in ['critical', 'degraded'] or signals:
                    risks.append({
                        'table': table,
                        'quality_score': quality_info['score'],
                        'status': quality_info['status'],
                        'recent_signals': len(signals),
                        'severity': 'high' if quality_info['status'] == 'critical' else 'medium'
                    })
        return risks

    def _find_critical_paths(self, source: str, downstream: List[str]) -> List[List[str]]:
        critical_paths = []
        for target in downstream[:5]:
            if self.graph.has_node(target):
                try:
                    paths = list(nx.all_simple_paths(self.graph, source, target, cutoff=5))
                    if paths:
                        critical_paths.append(paths[0])
                except nx.NetworkXNoPath:
                    continue
        return critical_paths

    def simulate_whatif(self, table: str, scenario: str) -> Dict[str, Any]:
        impact = self.analyze_change_impact(table, scenario)
        
        return {
            'scenario': scenario,
            'table': table,
            'predicted_impact': impact['impact_score'],
            'affected_tables': impact['downstream_count'],
            'recommendation': self._generate_recommendation(impact)
        }

    def _generate_recommendation(self, impact: Dict) -> str:
        if impact['impact_score'] > 70:
            return 'High risk: Consider feature flag or staged rollout'
        elif impact['impact_score'] > 40:
            return 'Medium risk: Test thoroughly and notify downstream owners'
        return 'Low risk: Proceed with standard deployment'
