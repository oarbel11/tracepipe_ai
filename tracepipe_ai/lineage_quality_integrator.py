from typing import Dict, List, Set
from tracepipe_ai.data_quality_monitor import (
    DataQualityMonitor, QualityIssue, QualityMetric
)
from datetime import datetime


class LineageQualityIntegrator:
    def __init__(self, quality_monitor: DataQualityMonitor):
        self.quality_monitor = quality_monitor
        self.lineage_graph: Dict[str, List[str]] = {}

    def add_lineage_edge(self, upstream: str, downstream: str):
        if upstream not in self.lineage_graph:
            self.lineage_graph[upstream] = []
        if downstream not in self.lineage_graph[upstream]:
            self.lineage_graph[upstream].append(downstream)

    def get_downstream_assets(self, asset_id: str) -> List[str]:
        visited = set()
        result = []

        def dfs(node: str):
            if node in visited:
                return
            visited.add(node)
            if node != asset_id:
                result.append(node)
            for downstream in self.lineage_graph.get(node, []):
                dfs(downstream)

        dfs(asset_id)
        return result

    def propagate_quality_issues(self, asset_id: str) -> List[str]:
        status = self.quality_monitor.get_asset_status(asset_id)
        if status == "unhealthy":
            affected = self.get_downstream_assets(asset_id)
            issue = QualityIssue(
                asset_id=asset_id,
                issue_type="data_quality",
                severity="high",
                message=f"Quality issue in {asset_id}",
                detected_at=datetime.now(),
                affected_downstream=affected
            )
            if asset_id not in self.quality_monitor.issues:
                self.quality_monitor.issues[asset_id] = []
            self.quality_monitor.issues[asset_id].append(issue)
            return affected
        return []

    def get_lineage_with_quality(self, asset_id: str) -> Dict:
        downstream = self.get_downstream_assets(asset_id)
        quality_overlay = {}
        quality_overlay[asset_id] = self.quality_monitor.get_asset_status(
            asset_id
        )
        for node in downstream:
            quality_overlay[node] = self.quality_monitor.get_asset_status(node)

        return {
            "root": asset_id,
            "downstream": downstream,
            "quality_status": quality_overlay
        }

    def get_blast_radius(self, asset_id: str) -> Dict:
        affected = self.propagate_quality_issues(asset_id)
        return {
            "source": asset_id,
            "affected_count": len(affected),
            "affected_assets": affected
        }
