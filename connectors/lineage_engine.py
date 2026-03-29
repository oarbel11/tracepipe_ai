from typing import List, Dict, Any
import networkx as nx
from .base import LineageEdge, BaseConnector


class LineageInferenceEngine:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.connectors: List[BaseConnector] = []

    def register_connector(self, connector: BaseConnector):
        self.connectors.append(connector)

    def build_lineage_graph(self, databricks_catalog: Dict) -> nx.DiGraph:
        self.graph.clear()
        
        for table_name in databricks_catalog:
            self.graph.add_node(table_name, system='databricks')
        
        all_edges = []
        for connector in self.connectors:
            try:
                edges = connector.infer_relationships(databricks_catalog)
                all_edges.extend(edges)
            except Exception as e:
                print(f"Connector error: {e}")
        
        for edge in all_edges:
            self.graph.add_node(edge.source_asset, system=edge.source_system)
            self.graph.add_node(edge.target_asset, system=edge.target_system)
            self.graph.add_edge(
                edge.source_asset,
                edge.target_asset,
                operation=edge.operation_type,
                metadata=edge.metadata
            )
        
        return self.graph

    def get_upstream_lineage(self, asset: str, max_depth: int = 5) -> List[str]:
        if asset not in self.graph:
            return []
        upstream = []
        for node in nx.ancestors(self.graph, asset):
            if len(upstream) >= max_depth:
                break
            upstream.append(node)
        return upstream

    def get_downstream_lineage(self, asset: str, max_depth: int = 5) -> List[str]:
        if asset not in self.graph:
            return []
        downstream = []
        for node in nx.descendants(self.graph, asset):
            if len(downstream) >= max_depth:
                break
            downstream.append(node)
        return downstream

    def get_cross_system_paths(self, source: str, target: str) -> List[List[str]]:
        if source not in self.graph or target not in self.graph:
            return []
        try:
            paths = list(nx.all_simple_paths(self.graph, source, target, cutoff=10))
            return paths[:5]
        except nx.NetworkXNoPath:
            return []

    def export_lineage(self) -> Dict[str, Any]:
        nodes = []
        edges = []
        
        for node, attrs in self.graph.nodes(data=True):
            nodes.append({"id": node, "system": attrs.get('system', 'unknown')})
        
        for source, target, attrs in self.graph.edges(data=True):
            edges.append({
                "source": source,
                "target": target,
                "operation": attrs.get('operation', 'unknown'),
                "metadata": attrs.get('metadata', {})
            })
        
        return {"nodes": nodes, "edges": edges}
