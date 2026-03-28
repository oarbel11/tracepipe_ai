from typing import Dict, List, Any
import networkx as nx
from .quality_monitor import QualityMonitor

class LineageQualityIntegrator:
    def __init__(self, quality_monitor: QualityMonitor):
        self.monitor = quality_monitor

    def enrich_lineage_graph(self, graph: nx.DiGraph) -> nx.DiGraph:
        enriched = graph.copy()
        
        for node in enriched.nodes():
            metrics = self.monitor.get_node_metrics(node)
            enriched.nodes[node]["quality_metrics"] = metrics
            enriched.nodes[node]["quality_status"] = self._aggregate_status(metrics)
        
        self._propagate_impact(enriched)
        return enriched

    def _aggregate_status(self, metrics: List[Dict]) -> str:
        if not metrics:
            return "unknown"
        statuses = [m["status"] for m in metrics]
        if "critical" in statuses:
            return "critical"
        if "warning" in statuses:
            return "warning"
        return "healthy"

    def _propagate_impact(self, graph: nx.DiGraph):
        for node in nx.topological_sort(graph):
            if graph.nodes[node].get("quality_status") in ["critical", "warning"]:
                for successor in graph.successors(node):
                    if "upstream_issues" not in graph.nodes[successor]:
                        graph.nodes[successor]["upstream_issues"] = []
                    graph.nodes[successor]["upstream_issues"].append({
                        "source": node,
                        "status": graph.nodes[node]["quality_status"]
                    })

    def get_impact_summary(self, graph: nx.DiGraph, source_node: str) -> Dict[str, Any]:
        if source_node not in graph:
            return {"error": "Node not found"}
        
        descendants = nx.descendants(graph, source_node)
        impacted = [n for n in descendants if graph.nodes[n].get("quality_status") in ["critical", "warning"]]
        
        return {
            "source": source_node,
            "source_status": graph.nodes[source_node].get("quality_status", "unknown"),
            "total_downstream": len(descendants),
            "impacted_downstream": len(impacted),
            "impacted_nodes": impacted,
            "metrics": graph.nodes[source_node].get("quality_metrics", [])
        }

    def get_root_causes(self, graph: nx.DiGraph, affected_node: str) -> List[str]:
        if affected_node not in graph:
            return []
        
        ancestors = nx.ancestors(graph, affected_node)
        problematic = [n for n in ancestors if graph.nodes[n].get("quality_status") in ["critical", "warning"]]
        
        roots = [n for n in problematic if not any(
            p in problematic for p in graph.predecessors(n)
        )]
        
        return roots
