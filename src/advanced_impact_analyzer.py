from typing import Dict, List, Set, Any, Optional
from .data_quality_integration import DataQualityMetrics


class AdvancedImpactAnalyzer:
    def __init__(self, quality_metrics: Optional[DataQualityMetrics] = None):
        self.lineage_graph = {}
        self.quality_metrics = quality_metrics or DataQualityMetrics()

    def add_dependency(self, source: str, target: str):
        if source not in self.lineage_graph:
            self.lineage_graph[source] = []
        if target not in self.lineage_graph[source]:
            self.lineage_graph[source].append(target)

    def get_downstream_impact(self, table: str) -> Set[str]:
        visited = set()
        self._traverse_downstream(table, visited)
        visited.discard(table)
        return visited

    def _traverse_downstream(self, node: str, visited: Set[str]):
        if node in visited:
            return
        visited.add(node)
        for child in self.lineage_graph.get(node, []):
            self._traverse_downstream(child, visited)

    def analyze_what_if(self, table: str) -> Dict[str, Any]:
        downstream = self.get_downstream_impact(table)
        affected_count = len(downstream)
        quality_impact = []
        for affected_table in downstream:
            score = self.quality_metrics.get_quality_score(affected_table)
            if score < 100.0:
                quality_impact.append({"table": affected_table, "score": score})
        return {
            "source_table": table,
            "affected_tables": list(downstream),
            "affected_count": affected_count,
            "quality_impact": quality_impact,
            "risk_level": self._calculate_risk(affected_count, quality_impact)
        }

    def _calculate_risk(self, count: int, quality_impact: List[Dict]) -> str:
        if count > 10 or len(quality_impact) > 5:
            return "high"
        elif count > 5 or len(quality_impact) > 2:
            return "medium"
        return "low"

    def get_root_cause_analysis(self, table: str) -> List[str]:
        upstream = []
        for source, targets in self.lineage_graph.items():
            if table in targets:
                upstream.append(source)
        return upstream
