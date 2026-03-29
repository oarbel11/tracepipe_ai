import networkx as nx
from typing import Dict, List, Set, Optional, Any


class ImpactAnalysisEngine:
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
        self.cache = {}

    def analyze_impact(
        self,
        asset_id: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        cache_key = (asset_id, str(filters))
        if cache_key in self.cache:
            return self.cache[cache_key]

        if asset_id not in self.graph:
            return {"error": "Asset not found", "impacted_assets": []}

        downstream = self._get_downstream_assets(asset_id)
        filtered = self._apply_filters(downstream, filters or {})
        impact_details = self._compute_impact_details(asset_id, filtered)

        result = {
            "source_asset": asset_id,
            "total_impacted": len(filtered),
            "impacted_assets": filtered,
            "impact_layers": impact_details["layers"],
            "critical_paths": impact_details["critical_paths"],
            "filters_applied": filters or {}
        }
        self.cache[cache_key] = result
        return result

    def _get_downstream_assets(self, asset_id: str) -> List[Dict[str, Any]]:
        descendants = nx.descendants(self.graph, asset_id)
        assets = []
        for node in descendants:
            node_data = self.graph.nodes[node]
            assets.append({
                "id": node,
                "type": node_data.get("type", "unknown"),
                "tags": node_data.get("tags", []),
                "owner": node_data.get("owner", "unassigned"),
                "quality_status": node_data.get("quality_status", "unknown"),
                "distance": nx.shortest_path_length(self.graph, asset_id, node)
            })
        return assets

    def _apply_filters(
        self,
        assets: List[Dict[str, Any]],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        filtered = assets
        if "tags" in filters:
            tag_filter = set(filters["tags"])
            filtered = [a for a in filtered if tag_filter & set(a["tags"])]
        if "owner" in filters:
            filtered = [a for a in filtered if a["owner"] == filters["owner"]]
        if "quality_status" in filters:
            filtered = [
                a for a in filtered
                if a["quality_status"] == filters["quality_status"]
            ]
        return filtered

    def _compute_impact_details(
        self,
        source: str,
        assets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        layers = {}
        for asset in assets:
            dist = asset["distance"]
            layers.setdefault(dist, []).append(asset["id"])

        critical_paths = self._find_critical_paths(source, assets)
        return {"layers": layers, "critical_paths": critical_paths}

    def _find_critical_paths(
        self,
        source: str,
        assets: List[Dict[str, Any]]
    ) -> List[List[str]]:
        critical = []
        for asset in assets:
            if "PII" in asset["tags"] or asset["quality_status"] == "critical":
                paths = list(nx.all_simple_paths(self.graph, source, asset["id"]))
                if paths:
                    critical.append(paths[0])
        return critical[:5]
