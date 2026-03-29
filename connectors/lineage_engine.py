"""Lineage inference engine for cross-system data flows."""

from typing import List, Dict, Set, Tuple
from .base import LineageEdge


class LineageInferenceEngine:
    """Engine to infer and map cross-system lineage."""

    def __init__(self):
        self.edges: List[LineageEdge] = []
        self.graph: Dict[str, Set[str]] = {}

    def add_edge(self, edge: LineageEdge) -> None:
        """Add a lineage edge to the graph."""
        self.edges.append(edge)
        source = f"{edge.source_system}:{edge.source_asset}"
        target = f"{edge.target_system}:{edge.target_asset}"
        
        if source not in self.graph:
            self.graph[source] = set()
        self.graph[source].add(target)

    def add_edges(self, edges: List[LineageEdge]) -> None:
        """Add multiple lineage edges."""
        for edge in edges:
            self.add_edge(edge)

    def get_downstream(self, system: str, asset: str) -> List[Tuple[str, str]]:
        """Get all downstream assets."""
        node = f"{system}:{asset}"
        if node not in self.graph:
            return []
        
        result = []
        for target in self.graph[node]:
            parts = target.split(':', 1)
            if len(parts) == 2:
                result.append((parts[0], parts[1]))
        return result

    def get_upstream(self, system: str, asset: str) -> List[Tuple[str, str]]:
        """Get all upstream assets."""
        target_node = f"{system}:{asset}"
        result = []
        
        for source, targets in self.graph.items():
            if target_node in targets:
                parts = source.split(':', 1)
                if len(parts) == 2:
                    result.append((parts[0], parts[1]))
        return result

    def get_all_edges(self) -> List[LineageEdge]:
        """Get all lineage edges."""
        return self.edges

    def clear(self) -> None:
        """Clear all lineage data."""
        self.edges = []
        self.graph = {}
