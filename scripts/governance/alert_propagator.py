from dataclasses import dataclass
from typing import List, Dict, Set
import networkx as nx
from scripts.peer_review.blast_radius import ImpactAnalysisMapper
from scripts.governance.policy_engine import PolicyViolation

@dataclass
class ImpactAlert:
    asset: str
    source_violation: str
    severity: str
    message: str
    owner: str
    impact_distance: int

class AlertPropagator:
    def __init__(self, lineage_graph: nx.DiGraph = None):
        self.lineage_graph = lineage_graph or nx.DiGraph()
        self.impact_mapper = ImpactAnalysisMapper(self.lineage_graph)
    
    def propagate_violations(self, violations: List[PolicyViolation]) -> List[ImpactAlert]:
        alerts = []
        
        for violation in violations:
            impacted_assets = self._get_downstream_assets(violation.asset_id)
            
            alerts.append(ImpactAlert(
                asset=violation.asset_id,
                source_violation=violation.policy_name,
                severity=violation.severity,
                message=f"[SOURCE] {violation.message}",
                owner=self._get_owner(violation.asset_id),
                impact_distance=0
            ))
            
            for asset, distance in impacted_assets.items():
                alerts.append(ImpactAlert(
                    asset=asset,
                    source_violation=violation.policy_name,
                    severity=self._propagate_severity(violation.severity, distance),
                    message=f"[DOWNSTREAM] Impacted by {violation.asset_id}: {violation.message}",
                    owner=self._get_owner(asset),
                    impact_distance=distance
                ))
        
        return alerts
    
    def _get_downstream_assets(self, asset_id: str) -> Dict[str, int]:
        if asset_id not in self.lineage_graph:
            return {}
        
        downstream = {}
        visited = set()
        queue = [(asset_id, 0)]
        
        while queue:
            current, distance = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            for successor in self.lineage_graph.successors(current):
                if successor not in downstream:
                    downstream[successor] = distance + 1
                    queue.append((successor, distance + 1))
        
        return downstream
    
    def _propagate_severity(self, severity: str, distance: int) -> str:
        severity_levels = {'critical': 3, 'high': 2, 'medium': 1, 'low': 0}
        level = severity_levels.get(severity, 1)
        degraded_level = max(0, level - distance)
        return {3: 'critical', 2: 'high', 1: 'medium', 0: 'low'}[degraded_level]
    
    def _get_owner(self, asset_id: str) -> str:
        if asset_id in self.lineage_graph.nodes:
            return self.lineage_graph.nodes[asset_id].get('owner', 'unknown')
        return 'unknown'
