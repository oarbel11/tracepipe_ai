from typing import Dict, List, Set, Optional
from datetime import datetime
from collections import defaultdict

class Alert:
    def __init__(self, asset_id: str, message: str, severity: str, affected_assets: List[str]):
        self.asset_id = asset_id
        self.message = message
        self.severity = severity
        self.affected_assets = affected_assets
        self.timestamp = datetime.utcnow().isoformat()

class LineageVersion:
    def __init__(self, asset_id: str, version: int, schema: Dict, upstream: List[str], downstream: List[str]):
        self.asset_id = asset_id
        self.version = version
        self.schema = schema
        self.upstream = upstream
        self.downstream = downstream
        self.timestamp = datetime.utcnow().isoformat()

class ImpactAnalyzer:
    def __init__(self):
        self.lineage_versions: Dict[str, List[LineageVersion]] = defaultdict(list)
        self.rename_map: Dict[str, str] = {}
        self.alerts: List[Alert] = []
        self.downstream_graph: Dict[str, Set[str]] = defaultdict(set)
        self.upstream_graph: Dict[str, Set[str]] = defaultdict(set)

    def track_lineage(self, asset_id: str, schema: Dict, upstream: List[str], downstream: List[str]):
        version = len(self.lineage_versions[asset_id]) + 1
        lineage = LineageVersion(asset_id, version, schema, upstream, downstream)
        self.lineage_versions[asset_id].append(lineage)
        
        self.downstream_graph[asset_id] = set(downstream)
        self.upstream_graph[asset_id] = set(upstream)
        for up in upstream:
            self.downstream_graph[up].add(asset_id)
        for down in downstream:
            self.upstream_graph[down].add(asset_id)

    def handle_rename(self, old_id: str, new_id: str):
        self.rename_map[old_id] = new_id
        if old_id in self.lineage_versions:
            self.lineage_versions[new_id] = self.lineage_versions[old_id]
            for version in self.lineage_versions[new_id]:
                version.asset_id = new_id

    def get_downstream_assets(self, asset_id: str) -> Set[str]:
        visited = set()
        queue = [asset_id]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            for downstream in self.downstream_graph.get(current, []):
                if downstream not in visited:
                    queue.append(downstream)
        visited.discard(asset_id)
        return visited

    def detect_schema_change(self, asset_id: str, new_schema: Dict) -> Optional[Alert]:
        if asset_id not in self.lineage_versions or not self.lineage_versions[asset_id]:
            return None
        last_version = self.lineage_versions[asset_id][-1]
        if last_version.schema != new_schema:
            affected = list(self.get_downstream_assets(asset_id))
            alert = Alert(asset_id, f"Schema changed for {asset_id}", "high", affected)
            self.alerts.append(alert)
            return alert
        return None

    def get_alerts(self) -> List[Alert]:
        return self.alerts
