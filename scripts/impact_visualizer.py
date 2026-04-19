import networkx as nx
from typing import Dict, List, Set
from scripts.lineage_ui_manager import LineageUIManager

class ImpactVisualizer:
    def __init__(self, lineage_manager: LineageUIManager):
        self.lineage_manager = lineage_manager

    def what_if_column_drop(self, asset_id: str, column_name: str) -> Dict:
        downstream = list(nx.descendants(self.lineage_manager.graph, asset_id)) if asset_id in self.lineage_manager.graph else []
        impact_map = {}
        for downstream_asset in downstream:
            edge_data = self.lineage_manager.graph.get_edge_data(asset_id, downstream_asset)
            if edge_data and edge_data.get("transform"):
                transform = edge_data["transform"]
                if column_name.lower() in transform.lower():
                    impact_map[downstream_asset] = "BROKEN - references dropped column"
                else:
                    impact_map[downstream_asset] = "OK - no direct reference"
            else:
                impact_map[downstream_asset] = "UNKNOWN - no transform info"
        return {
            "change": f"drop column {column_name}",
            "asset": asset_id,
            "downstream_impact": impact_map,
            "total_affected": len([v for v in impact_map.values() if "BROKEN" in v])
        }

    def what_if_column_add(self, asset_id: str, column_name: str, column_type: str) -> Dict:
        downstream = list(nx.descendants(self.lineage_manager.graph, asset_id)) if asset_id in self.lineage_manager.graph else []
        return {
            "change": f"add column {column_name} ({column_type})",
            "asset": asset_id,
            "downstream_count": len(downstream),
            "recommendation": "Safe - new columns don't break existing transforms" if downstream else "No downstream impact"
        }

    def what_if_rename_asset(self, old_name: str, new_name: str) -> Dict:
        if old_name not in self.lineage_manager.graph:
            return {"error": "Asset not found in lineage"}
        downstream = list(nx.descendants(self.lineage_manager.graph, old_name))
        upstream = list(nx.ancestors(self.lineage_manager.graph, old_name))
        return {
            "change": f"rename {old_name} to {new_name}",
            "affected_downstream": downstream,
            "affected_upstream": upstream,
            "total_impacted": len(downstream) + len(upstream),
            "action_required": "Update all SQL references and re-run dependent jobs"
        }

    def calculate_blast_radius(self, asset_id: str) -> Dict:
        if asset_id not in self.lineage_manager.graph:
            return {"blast_radius": 0, "assets": []}
        downstream = nx.descendants(self.lineage_manager.graph, asset_id)
        levels = {}
        for node in downstream:
            try:
                path_length = nx.shortest_path_length(self.lineage_manager.graph, asset_id, node)
                if path_length not in levels:
                    levels[path_length] = []
                levels[path_length].append(node)
            except nx.NetworkXNoPath:
                pass
        return {
            "blast_radius": len(downstream),
            "assets": list(downstream),
            "levels": levels,
            "max_depth": max(levels.keys()) if levels else 0
        }

    def get_critical_path(self, source: str, target: str) -> Dict:
        try:
            path = nx.shortest_path(self.lineage_manager.graph, source, target)
            return {"path": path, "length": len(path) - 1, "exists": True}
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return {"path": [], "length": 0, "exists": False}
