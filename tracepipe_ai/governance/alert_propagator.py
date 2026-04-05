from typing import Dict, List, Set, Any
from .violation_detector import Violation

class Alert:
    def __init__(self, asset_id: str, violation: Violation,
                 impact_level: str, owners: List[str]):
        self.asset_id = asset_id
        self.violation = violation
        self.impact_level = impact_level
        self.owners = owners

class AlertPropagator:
    def __init__(self, lineage_graph: Dict[str, List[str]]):
        self.lineage_graph = lineage_graph
        self.alerts: List[Alert] = []

    def get_downstream_assets(self, asset_id: str) -> Set[str]:
        visited = set()
        queue = [asset_id]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            downstream = self.lineage_graph.get(current, [])
            queue.extend([d for d in downstream if d not in visited])
        visited.discard(asset_id)
        return visited

    def propagate_alert(self, violation: Violation,
                        owners_map: Dict[str, List[str]]) -> List[Alert]:
        alerts = []
        source_asset = violation.asset_id
        source_owners = owners_map.get(source_asset, [])
        alerts.append(Alert(source_asset, violation, 'critical', source_owners))
        downstream = self.get_downstream_assets(source_asset)
        for asset in downstream:
            owners = owners_map.get(asset, [])
            alerts.append(Alert(asset, violation, 'warning', owners))
        self.alerts.extend(alerts)
        return alerts

    def get_alerts_for_asset(self, asset_id: str) -> List[Alert]:
        return [a for a in self.alerts if a.asset_id == asset_id]

    def get_alerts_for_owner(self, owner: str) -> List[Alert]:
        return [a for a in self.alerts if owner in a.owners]

    def clear_alerts(self):
        self.alerts.clear()
