"""Manages lineage UI and visualization."""
import json
from typing import Dict, List, Set, Any, Optional


class LineageUIManager:
    """Manages interactive lineage UI and exploration."""

    def __init__(self):
        self.lineage_graph = {}
        self.metadata = {}

    def add_lineage(self, source: str, target: str, metadata: Dict = None):
        """Add lineage relationship."""
        if source not in self.lineage_graph:
            self.lineage_graph[source] = {"downstream": [], "upstream": []}
        if target not in self.lineage_graph:
            self.lineage_graph[target] = {"downstream": [], "upstream": []}

        self.lineage_graph[source]["downstream"].append(target)
        self.lineage_graph[target]["upstream"].append(source)

        if metadata:
            key = f"{source}->{target}"
            self.metadata[key] = metadata

    def get_downstream(self, entity: str, depth: int = -1) -> List[str]:
        """Get downstream entities."""
        visited = set()
        result = []
        self._traverse_downstream(entity, depth, 0, visited, result)
        return result

    def _traverse_downstream(self, entity: str, max_depth: int, current_depth: int,
                            visited: Set, result: List):
        if entity in visited or (max_depth != -1 and current_depth > max_depth):
            return
        visited.add(entity)
        if entity in self.lineage_graph:
            for downstream in self.lineage_graph[entity]["downstream"]:
                if downstream not in result:
                    result.append(downstream)
                self._traverse_downstream(downstream, max_depth, current_depth + 1,
                                         visited, result)

    def get_upstream(self, entity: str, depth: int = -1) -> List[str]:
        """Get upstream entities."""
        visited = set()
        result = []
        self._traverse_upstream(entity, depth, 0, visited, result)
        return result

    def _traverse_upstream(self, entity: str, max_depth: int, current_depth: int,
                          visited: Set, result: List):
        if entity in visited or (max_depth != -1 and current_depth > max_depth):
            return
        visited.add(entity)
        if entity in self.lineage_graph:
            for upstream in self.lineage_graph[entity]["upstream"]:
                if upstream not in result:
                    result.append(upstream)
                self._traverse_upstream(upstream, max_depth, current_depth + 1,
                                       visited, result)

    def get_lineage_subgraph(self, entity: str, depth: int = 2) -> Dict:
        """Get lineage subgraph around entity."""
        upstream = self.get_upstream(entity, depth)
        downstream = self.get_downstream(entity, depth)
        nodes = set([entity] + upstream + downstream)
        edges = []
        for node in nodes:
            if node in self.lineage_graph:
                for target in self.lineage_graph[node]["downstream"]:
                    if target in nodes:
                        edges.append({"source": node, "target": target})
        return {"nodes": list(nodes), "edges": edges}

    def validate_lineage(self) -> List[Dict]:
        """Validate lineage integrity."""
        issues = []
        for entity, connections in self.lineage_graph.items():
            if not connections["upstream"] and not connections["downstream"]:
                issues.append({"entity": entity, "issue": "isolated_node"})
        return issues
