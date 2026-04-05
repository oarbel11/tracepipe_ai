import networkx as nx
import json
from .governance import PolicyEngine


class ImpactAnalysisResult:
    def __init__(self, root_asset, affected_assets, policies_map):
        self.root_asset = root_asset
        self.affected_assets = affected_assets
        self.policies_map = policies_map

    def to_json(self):
        return json.dumps({
            "root_asset": self.root_asset,
            "affected_count": len(self.affected_assets),
            "affected_assets": self.affected_assets,
            "policies": self.policies_map
        }, indent=2)


class InteractiveImpactAnalyzer:
    def __init__(self, policy_engine=None):
        self.graph = nx.DiGraph()
        self.metadata_store = {}
        self.policy_engine = policy_engine or PolicyEngine()
        self.policy_engine.load_default_policies()

    def add_asset(self, name, metadata=None):
        self.graph.add_node(name)
        self.metadata_store[name] = metadata or {}

    def add_dependency(self, upstream, downstream):
        self.graph.add_edge(upstream, downstream)

    def analyze_impact(self, asset_name, filters=None, policies=None, max_depth=None):
        if asset_name not in self.graph:
            return ImpactAnalysisResult(asset_name, [], {})

        filters = filters or {}
        if policies:
            for policy in policies:
                self.policy_engine.add_policy(policy)

        affected = self._get_downstream_assets(asset_name, max_depth)
        filtered_affected = self._apply_filters(affected, filters)
        policies_map = self._build_policies_map(filtered_affected)

        result_assets = [{
            "name": asset,
            "metadata": self.metadata_store.get(asset, {}),
            "policies": [p.to_dict() for p in policies_map.get(asset, [])]
        } for asset in filtered_affected]

        return ImpactAnalysisResult(asset_name, result_assets, policies_map)

    def _get_downstream_assets(self, asset_name, max_depth):
        if max_depth is None:
            return list(nx.descendants(self.graph, asset_name))
        affected = set()
        queue = [(asset_name, 0)]
        visited = {asset_name}
        while queue:
            current, depth = queue.pop(0)
            if depth < max_depth:
                for successor in self.graph.successors(current):
                    if successor not in visited:
                        visited.add(successor)
                        affected.add(successor)
                        queue.append((successor, depth + 1))
        return list(affected)

    def _apply_filters(self, assets, filters):
        filtered = assets
        if "tags" in filters:
            filtered = [a for a in filtered if any(
                tag in self.metadata_store.get(a, {}).get("tags", []) 
                for tag in filters["tags"]
            )]
        if "owner" in filters:
            filtered = [a for a in filtered if 
                self.metadata_store.get(a, {}).get("owner") == filters["owner"]]
        if "quality_status" in filters:
            filtered = [a for a in filtered if 
                self.metadata_store.get(a, {}).get("quality_status") == filters["quality_status"]]
        return filtered

    def _build_policies_map(self, assets):
        policies_map = {}
        for asset in assets:
            metadata = self.metadata_store.get(asset, {})
            policies_map[asset] = self.policy_engine.get_applicable_policies(metadata)
        return policies_map
