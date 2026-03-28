import networkx as nx
from typing import Dict, List, Any, Optional
from .connector_registry import ConnectorRegistry


class LineageGraphBuilder:
    def __init__(self, registry: ConnectorRegistry):
        self.registry = registry
        self.graph = nx.DiGraph()

    def build_graph(self) -> nx.DiGraph:
        self.graph.clear()
        lineage_data = self.registry.extract_all_lineage()
        
        for connector_id, lineage_items in lineage_data.items():
            for item in lineage_items:
                self._add_lineage_edge(item)
        
        return self.graph

    def _add_lineage_edge(self, item: Dict[str, Any]):
        source = item.get('source')
        target = item.get('target')
        metadata = item.get('metadata', {})
        
        if source and target:
            self.graph.add_node(source, **metadata.get('source_attrs', {}))
            self.graph.add_node(target, **metadata.get('target_attrs', {}))
            self.graph.add_edge(source, target, **metadata.get('edge_attrs', {}))

    def get_upstream(self, node: str, depth: int = -1) -> List[str]:
        if node not in self.graph:
            return []
        if depth == -1:
            return list(nx.ancestors(self.graph, node))
        return [n for n in self.graph.nodes() 
                if nx.has_path(self.graph, n, node) and 
                nx.shortest_path_length(self.graph, n, node) <= depth]

    def get_downstream(self, node: str, depth: int = -1) -> List[str]:
        if node not in self.graph:
            return []
        if depth == -1:
            return list(nx.descendants(self.graph, node))
        return [n for n in self.graph.nodes() 
                if nx.has_path(self.graph, node, n) and 
                nx.shortest_path_length(self.graph, node, n) <= depth]

    def get_full_path(self, source: str, target: str) -> List[List[str]]:
        if source not in self.graph or target not in self.graph:
            return []
        try:
            return list(nx.all_simple_paths(self.graph, source, target))
        except nx.NetworkXNoPath:
            return []

    def export_graph(self) -> Dict[str, Any]:
        return {
            'nodes': [{'id': n, **self.graph.nodes[n]} for n in self.graph.nodes()],
            'edges': [{'source': u, 'target': v, **self.graph.edges[u, v]} 
                     for u, v in self.graph.edges()]
        }
