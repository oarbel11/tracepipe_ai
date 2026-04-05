"""Graph structure for operational lineage."""

from typing import Dict, List, Set, Any, Optional


class LineageNode:
    """Represents a node in the lineage graph."""

    def __init__(self, node_id: str, node_type: str, metadata: Dict = None):
        self.node_id = node_id
        self.node_type = node_type
        self.metadata = metadata or {}


class LineageGraph:
    """Lightweight graph structure for operational lineage."""

    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[Dict[str, str]] = []

    def add_node(self, node_id: str, node_type: str,
                 metadata: Optional[Dict] = None) -> None:
        """Add a node to the graph."""
        if node_id not in self.nodes:
            self.nodes[node_id] = LineageNode(node_id, node_type, metadata)

    def add_edge(self, source: str, target: str, edge_type: str = 'produces'):
        """Add an edge between two nodes."""
        edge = {'source': source, 'target': target, 'type': edge_type}
        if edge not in self.edges:
            self.edges.append(edge)

    def build_from_records(self, records: List[Dict[str, Any]]) -> None:
        """Build graph from lineage records."""
        for record in records:
            asset_id = record['asset_id']
            asset_type = record['type']
            self.add_node(asset_id, asset_type, record)

            for table in record.get('tables_read', []):
                self.add_node(table, 'table')
                self.add_edge(table, asset_id, 'consumed_by')

            for table in record.get('tables_written', []):
                self.add_node(table, 'table')
                self.add_edge(asset_id, table, 'produces')

    def get_downstream(self, node_id: str) -> List[str]:
        """Get all downstream nodes from a given node."""
        downstream = []
        for edge in self.edges:
            if edge['source'] == node_id:
                downstream.append(edge['target'])
        return downstream

    def get_upstream(self, node_id: str) -> List[str]:
        """Get all upstream nodes from a given node."""
        upstream = []
        for edge in self.edges:
            if edge['target'] == node_id:
                upstream.append(edge['source'])
        return upstream

    def get_nodes_by_type(self, node_type: str) -> List[str]:
        """Get all nodes of a specific type."""
        return [nid for nid, node in self.nodes.items()
                if node.node_type == node_type]
