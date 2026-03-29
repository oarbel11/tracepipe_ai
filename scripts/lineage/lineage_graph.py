import networkx as nx
from typing import Dict, List, Optional, Tuple


class LineageGraphBuilder:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.node_metadata = {}

    def add_table_node(self, node_id: str, platform: str, schema: str,
                       table: str, metadata: Optional[Dict] = None):
        full_name = f"{platform}.{schema}.{table}"
        self.graph.add_node(node_id, type="table", name=full_name,
                           platform=platform, schema=schema, table=table)
        if metadata:
            self.node_metadata[node_id] = metadata

    def add_column_node(self, node_id: str, table_id: str, column: str,
                        data_type: str, metadata: Optional[Dict] = None):
        self.graph.add_node(node_id, type="column", column=column,
                           data_type=data_type, parent=table_id)
        self.graph.add_edge(table_id, node_id, relationship="contains")
        if metadata:
            self.node_metadata[node_id] = metadata

    def add_lineage_edge(self, source_id: str, target_id: str,
                        transformation: Optional[str] = None):
        attrs = {"relationship": "flows_to"}
        if transformation:
            attrs["transformation"] = transformation
        self.graph.add_edge(source_id, target_id, **attrs)

    def get_upstream(self, node_id: str, depth: int = -1) -> List[str]:
        if node_id not in self.graph:
            return []
        if depth == -1:
            return list(nx.ancestors(self.graph, node_id))
        upstream = []
        for i in range(depth):
            predecessors = list(self.graph.predecessors(node_id))
            upstream.extend(predecessors)
            if not predecessors:
                break
        return list(set(upstream))

    def get_downstream(self, node_id: str, depth: int = -1) -> List[str]:
        if node_id not in self.graph:
            return []
        if depth == -1:
            return list(nx.descendants(self.graph, node_id))
        downstream = []
        for i in range(depth):
            successors = list(self.graph.successors(node_id))
            downstream.extend(successors)
            if not successors:
                break
        return list(set(downstream))

    def find_path(self, source_id: str, target_id: str) -> List[List[str]]:
        try:
            paths = nx.all_simple_paths(self.graph, source_id, target_id)
            return [list(p) for p in paths]
        except (nx.NodeNotFound, nx.NetworkXNoPath):
            return []

    def get_impact_analysis(self, node_id: str) -> Dict:
        return {
            "node": node_id,
            "upstream_count": len(self.get_upstream(node_id)),
            "downstream_count": len(self.get_downstream(node_id)),
            "downstream_nodes": self.get_downstream(node_id)
        }

    def export_graph(self) -> Dict:
        return {
            "nodes": dict(self.graph.nodes(data=True)),
            "edges": list(self.graph.edges(data=True)),
            "metadata": self.node_metadata
        }
