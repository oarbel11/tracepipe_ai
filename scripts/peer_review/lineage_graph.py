"""Lineage graph structure for Tracepipe AI."""

class LineageNode:
    def __init__(self, node_id: str, node_type: str, metadata: dict = None):
        self.node_id = node_id
        self.node_type = node_type
        self.metadata = metadata or {}
        self.tags = self.metadata.get('tags', [])
        self.owner = self.metadata.get('owner', '')
        self.quality_status = self.metadata.get('quality_status', 'unknown')

class LineageGraph:
    def __init__(self):
        self.nodes = {}
        self.edges = []

    def add_node(self, node: LineageNode):
        self.nodes[node.node_id] = node

    def add_edge(self, source_id: str, target_id: str):
        self.edges.append((source_id, target_id))

    def get_node(self, node_id: str):
        return self.nodes.get(node_id)

    def get_downstream_nodes(self, node_id: str):
        downstream = []
        for source, target in self.edges:
            if source == node_id:
                downstream.append(target)
        return downstream

    def get_upstream_nodes(self, node_id: str):
        upstream = []
        for source, target in self.edges:
            if target == node_id:
                upstream.append(source)
        return upstream
