from typing import Dict, List, Set, Optional
from scripts.lineage_graph import LineageGraph
import networkx as nx

class ImpactAnalyzer:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def analyze_impact(self, node_id: str, max_depth: int = -1) -> Dict:
        resolved = self.graph.resolve_node(node_id)
        if not resolved:
            return {
                "node_id": node_id,
                "exists": False,
                "upstream_nodes": [],
                "downstream_nodes": [],
                "impact_score": 0
            }

        upstream = self.graph.get_upstream(resolved, max_depth)
        downstream = self.graph.get_downstream(resolved, max_depth)

        upstream_details = self._get_node_details(upstream)
        downstream_details = self._get_node_details(downstream)

        impact_score = len(downstream) * 10 + len(upstream) * 5

        return {
            "node_id": resolved,
            "original_node_id": node_id,
            "exists": True,
            "upstream_nodes": upstream_details,
            "downstream_nodes": downstream_details,
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
            "impact_score": impact_score,
            "critical_downstream": self._identify_critical_nodes(downstream)
        }

    def _get_node_details(self, node_ids: Set[str]) -> List[Dict]:
        details = []
        for node_id in node_ids:
            if self.graph.graph.has_node(node_id):
                node_data = self.graph.graph.nodes[node_id]
                details.append({
                    "node_id": node_id,
                    "node_type": node_data.get("node_type", "unknown"),
                    "system": node_data.get("system", "unknown")
                })
        return details

    def _identify_critical_nodes(self, node_ids: Set[str]) -> List[str]:
        critical = []
        for node_id in node_ids:
            if self.graph.graph.has_node(node_id):
                node_data = self.graph.graph.nodes[node_id]
                if node_data.get("node_type") in ["dashboard", "report"]:
                    critical.append(node_id)
                elif "critical" in node_data.get("tags", []):
                    critical.append(node_id)
        return critical

    def find_root_causes(self, node_id: str) -> List[str]:
        resolved = self.graph.resolve_node(node_id)
        if not resolved:
            return []
        upstream = self.graph.get_upstream(resolved)
        roots = [n for n in upstream if self.graph.graph.in_degree(n) == 0]
        return roots

    def get_path(self, source: str, target: str) -> Optional[List[str]]:
        source_resolved = self.graph.resolve_node(source)
        target_resolved = self.graph.resolve_node(target)
        if not source_resolved or not target_resolved:
            return None
        try:
            return nx.shortest_path(self.graph.graph, source_resolved, target_resolved)
        except nx.NetworkXNoPath:
            return None
