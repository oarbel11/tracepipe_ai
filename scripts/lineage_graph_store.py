class LineageGraphStore:
    def __init__(self):
        self.nodes = {}
        self.edges = []
        self.node_index = {}

    def add_node(self, node_id, node_type, metadata=None):
        node = {
            'id': node_id,
            'type': node_type,
            'metadata': metadata or {}
        }
        self.nodes[node_id] = node
        if node_type not in self.node_index:
            self.node_index[node_type] = set()
        self.node_index[node_type].add(node_id)
        return node

    def add_edge(self, source_id, target_id, edge_type='depends_on'):
        edge = {
            'source': source_id,
            'target': target_id,
            'type': edge_type
        }
        self.edges.append(edge)
        return edge

    def get_node(self, node_id):
        return self.nodes.get(node_id)

    def get_nodes_by_type(self, node_type):
        node_ids = self.node_index.get(node_type, set())
        return [self.nodes[nid] for nid in node_ids]

    def query_nodes(self, filters):
        results = []
        for node in self.nodes.values():
            match = True
            for key, value in filters.items():
                if key == 'type':
                    if node['type'] != value:
                        match = False
                        break
                elif key in node['metadata']:
                    if node['metadata'][key] != value:
                        match = False
                        break
                else:
                    match = False
                    break
            if match:
                results.append(node)
        return results

    def get_downstream_nodes(self, node_id):
        downstream = []
        for edge in self.edges:
            if edge['source'] == node_id:
                target = self.nodes.get(edge['target'])
                if target:
                    downstream.append(target)
        return downstream

    def clear(self):
        self.nodes = {}
        self.edges = []
        self.node_index = {}
