import json
import time
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
import networkx as nx


@dataclass
class LineageVersion:
    version: int
    timestamp: float
    upstream: List[str]
    downstream: List[str]
    metadata: Dict = field(default_factory=dict)


@dataclass
class Asset:
    asset_id: str
    current_name: str
    previous_names: List[str] = field(default_factory=list)
    versions: List[LineageVersion] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)


@dataclass
class Alert:
    alert_id: str
    asset_id: str
    alert_type: str
    severity: str
    message: str
    affected_downstream: List[str]
    timestamp: float
    resolved: bool = False


class ImpactAnalyzer:
    def __init__(self):
        self.assets: Dict[str, Asset] = {}
        self.name_mapping: Dict[str, str] = {}
        self.graph = nx.DiGraph()
        self.alerts: Dict[str, Alert] = {}

    def track_lineage(self, asset_name: str, upstream: List[str],
                      downstream: List[str] = None, tags: List[str] = None):
        downstream = downstream or []
        tags = tags or []
        asset_id = self.name_mapping.get(asset_name, asset_name)
        
        if asset_id not in self.assets:
            self.assets[asset_id] = Asset(asset_id, asset_name, [], [], tags)
            self.name_mapping[asset_name] = asset_id
        
        asset = self.assets[asset_id]
        version = LineageVersion(
            version=len(asset.versions) + 1,
            timestamp=time.time(),
            upstream=upstream,
            downstream=downstream
        )
        asset.versions.append(version)
        
        self.graph.add_node(asset_name)
        for up in upstream:
            self.graph.add_edge(up, asset_name)
        for down in downstream:
            self.graph.add_edge(asset_name, down)

    def handle_rename(self, old_name: str, new_name: str):
        asset_id = self.name_mapping.get(old_name, old_name)
        if asset_id in self.assets:
            asset = self.assets[asset_id]
            asset.previous_names.append(asset.current_name)
            asset.current_name = new_name
            self.name_mapping[new_name] = asset_id
            nx.relabel_nodes(self.graph, {old_name: new_name}, copy=False)

    def get_downstream_impact(self, asset_name: str) -> Dict:
        asset_id = self.name_mapping.get(asset_name, asset_name)
        if asset_name not in self.graph:
            return {"affected_assets": [], "depth": 0}
        
        descendants = nx.descendants(self.graph, asset_name)
        return {
            "asset": asset_name,
            "affected_assets": list(descendants),
            "depth": max([nx.shortest_path_length(self.graph, asset_name, d)
                         for d in descendants], default=0)
        }

    def create_alert(self, asset_name: str, alert_type: str,
                     message: str, severity: str = "medium"):
        impact = self.get_downstream_impact(asset_name)
        alert_id = f"{asset_name}_{alert_type}_{int(time.time())}"
        alert = Alert(
            alert_id=alert_id,
            asset_id=self.name_mapping.get(asset_name, asset_name),
            alert_type=alert_type,
            severity=severity,
            message=message,
            affected_downstream=impact["affected_assets"],
            timestamp=time.time()
        )
        self.alerts[alert_id] = alert
        return alert

    def get_active_alerts(self) -> List[Dict]:
        return [asdict(a) for a in self.alerts.values() if not a.resolved]
