"""Integrate quality metrics with lineage graphs."""

from typing import Dict, Any, List, Optional


class LineageIntegrator:
    """Overlay quality data on lineage graphs."""

    def __init__(self, quality_monitor):
        """Initialize integrator."""
        self.quality_monitor = quality_monitor

    def enrich_lineage_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a lineage node with quality metrics."""
        node_id = node.get("id")
        if not node_id:
            return node

        enriched = node.copy()
        metrics = self.quality_monitor.get_node_metrics(node_id, limit=5)
        alerts = self.quality_monitor.get_alerts_for_node(node_id)

        enriched["quality_metrics"] = metrics
        enriched["quality_alerts"] = alerts
        enriched["quality_status"] = self._compute_overall_status(metrics)

        return enriched

    def enrich_lineage_graph(self, graph: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich entire lineage graph with quality data."""
        enriched = graph.copy()
        
        if "nodes" in enriched:
            enriched["nodes"] = [
                self.enrich_lineage_node(node) for node in enriched["nodes"]
            ]

        enriched["quality_summary"] = self._generate_summary()

        return enriched

    def _compute_overall_status(self, metrics: List[Dict[str, Any]]) -> str:
        """Compute overall quality status from metrics."""
        if not metrics:
            return "unknown"

        statuses = [m.get("status") for m in metrics if "status" in m]
        
        if "critical" in statuses:
            return "critical"
        elif "warning" in statuses:
            return "warning"
        elif "healthy" in statuses:
            return "healthy"
        
        return "unknown"

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate quality summary for entire graph."""
        alerts = self.quality_monitor.get_active_alerts()
        
        critical = sum(1 for a in alerts if a.get("status") == "critical")
        warnings = sum(1 for a in alerts if a.get("status") == "warning")

        return {
            "total_alerts": len(alerts),
            "critical_alerts": critical,
            "warning_alerts": warnings
        }
