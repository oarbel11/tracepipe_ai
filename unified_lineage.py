class LineageNode:
    def __init__(self, node_id, node_type, metadata=None):
        self.node_id = node_id
        self.node_type = node_type
        self.metadata = metadata or {}

    def __repr__(self):
        return f"LineageNode({self.node_id}, {self.node_type})"


class LineageEdge:
    def __init__(self, source, target, edge_type="default", metadata=None):
        self.source = source
        self.target = target
        self.edge_type = edge_type
        self.metadata = metadata or {}

    def __repr__(self):
        return f"LineageEdge({self.source} -> {self.target})"


class LineageGraph:
    def __init__(self):
        self.nodes = {}
        self.edges = []

    def add_node(self, node):
        self.nodes[node.node_id] = node

    def add_edge(self, edge):
        self.edges.append(edge)

    def get_upstream(self, node_id, visited=None):
        if visited is None:
            visited = set()
        if node_id in visited:
            return set()
        visited.add(node_id)
        upstream = set()
        for edge in self.edges:
            if edge.target == node_id:
                upstream.add(edge.source)
                upstream.update(self.get_upstream(edge.source, visited))
        return upstream

    def get_downstream(self, node_id, visited=None):
        if visited is None:
            visited = set()
        if node_id in visited:
            return set()
        visited.add(node_id)
        downstream = set()
        for edge in self.edges:
            if edge.source == node_id:
                downstream.add(edge.target)
                downstream.update(self.get_downstream(edge.target, visited))
        return downstream

    def merge(self, other_graph):
        for node in other_graph.nodes.values():
            if node.node_id not in self.nodes:
                self.add_node(node)
        for edge in other_graph.edges:
            self.add_edge(edge)

    def get_path(self, start_id, end_id):
        visited = set()
        queue = [(start_id, [start_id])]
        while queue:
            current, path = queue.pop(0)
            if current == end_id:
                return path
            if current in visited:
                continue
            visited.add(current)
            for edge in self.edges:
                if edge.source == current and edge.target not in visited:
                    queue.append((edge.target, path + [edge.target]))
        return None
