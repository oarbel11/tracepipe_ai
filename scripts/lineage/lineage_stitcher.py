import networkx as nx
from typing import Dict, List, Set, Any, Tuple
from .external_connectors import ExternalConnectorRegistry


class LineageStitcher:
    def __init__(self, unity_catalog_lineage: List[Dict[str, Any]]):
        self.uc_lineage = unity_catalog_lineage
        self.registry = ExternalConnectorRegistry()
        self.graph = nx.DiGraph()

    def add_unity_catalog_lineage(self):
        for edge in self.uc_lineage:
            source = edge.get('source', '')
            target = edge.get('target', '')
            if source and target:
                self.graph.add_edge(
                    source,
                    target,
                    system='unity_catalog',
                    metadata=edge
                )

    def add_external_lineage(self, external_configs: List[Dict[str, Any]]):
        for config in external_configs:
            system_type = config.get('type')
            connector = self.registry.get_connector(system_type, config)
            if connector:
                lineage_edges = connector.extract_lineage()
                for edge in lineage_edges:
                    self._add_edge_with_normalization(edge)

    def _add_edge_with_normalization(self, edge: Dict[str, Any]):
        sources = edge.get('source')
        target = edge.get('target')
        system = edge.get('system')
        
        if isinstance(sources, list):
            for src in sources:
                if src and target:
                    self.graph.add_edge(src, target, system=system, metadata=edge)
        elif sources and target:
            self.graph.add_edge(sources, target, system=system, metadata=edge)

    def stitch_lineage(self, external_configs: List[Dict[str, Any]]) -> nx.DiGraph:
        self.add_unity_catalog_lineage()
        self.add_external_lineage(external_configs)
        return self.graph

    def get_end_to_end_path(self, source: str, target: str) -> List[List[str]]:
        try:
            return list(nx.all_simple_paths(self.graph, source, target))
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

    def get_upstream_dependencies(self, node: str, max_depth: int = 5) -> Set[str]:
        upstream = set()
        try:
            for i in range(1, max_depth + 1):
                for pred in nx.ancestors(self.graph, node):
                    upstream.add(pred)
        except nx.NodeNotFound:
            pass
        return upstream

    def get_downstream_impact(self, node: str) -> Set[str]:
        try:
            return set(nx.descendants(self.graph, node))
        except nx.NodeNotFound:
            return set()

    def export_lineage(self) -> Dict[str, Any]:
        return {
            'nodes': list(self.graph.nodes()),
            'edges': [{
                'source': u,
                'target': v,
                'system': data.get('system'),
                'metadata': data.get('metadata', {})
            } for u, v, data in self.graph.edges(data=True)],
            'stats': {
                'total_nodes': self.graph.number_of_nodes(),
                'total_edges': self.graph.number_of_edges()
            }
        }
