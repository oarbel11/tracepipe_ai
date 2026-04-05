from typing import Dict, List, Any, Optional
from .advanced_impact_analyzer import AdvancedImpactAnalyzer
from .data_quality_integration import DataQualityMetrics


class BlastRadiusAnalyzer:
    def __init__(self):
        self.quality_metrics = DataQualityMetrics()
        self.impact_analyzer = AdvancedImpactAnalyzer(self.quality_metrics)

    def add_lineage(self, source: str, target: str):
        self.impact_analyzer.add_dependency(source, target)

    def add_quality_metric(self, table: str, metric_type: str, value: float,
                          threshold: Optional[float] = None):
        self.quality_metrics.add_metric(table, metric_type, value, threshold)

    def analyze_blast_radius(self, table: str) -> Dict[str, Any]:
        what_if = self.impact_analyzer.analyze_what_if(table)
        alerts = self.quality_metrics.get_alerts(table)
        upstream = self.impact_analyzer.get_root_cause_analysis(table)
        return {
            "table": table,
            "downstream_impact": what_if["affected_tables"],
            "upstream_dependencies": upstream,
            "affected_count": what_if["affected_count"],
            "risk_level": what_if["risk_level"],
            "quality_score": self.quality_metrics.get_quality_score(table),
            "quality_alerts": alerts,
            "quality_impact": what_if["quality_impact"]
        }

    def get_lineage_with_quality(self, table: str) -> Dict[str, Any]:
        blast_radius = self.analyze_blast_radius(table)
        enriched_downstream = []
        for downstream_table in blast_radius["downstream_impact"]:
            enriched_downstream.append({
                "table": downstream_table,
                "quality_score": self.quality_metrics.get_quality_score(
                    downstream_table),
                "alerts": self.quality_metrics.get_alerts(downstream_table)
            })
        return {
            "table": table,
            "downstream": enriched_downstream,
            "upstream": blast_radius["upstream_dependencies"],
            "quality_score": blast_radius["quality_score"],
            "risk_level": blast_radius["risk_level"]
        }
