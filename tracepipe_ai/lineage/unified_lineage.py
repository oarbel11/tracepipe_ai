class UnifiedLineageGraph:
    def __init__(self):
        self.nodes = {}
        self.edges = []

    def add_node(self, node_id, metadata=None):
        self.nodes[node_id] = metadata or {}

    def add_edge(self, source, target, edge_type='default'):
        self.edges.append({
            'source': source,
            'target': target,
            'type': edge_type
        })

    def get_upstream(self, node_id, depth=None):
        visited = set()
        to_visit = [node_id]
        current_depth = 0
        
        while to_visit:
            if depth is not None and current_depth >= depth:
                break
            next_level = []
            for current in to_visit:
                if current in visited:
                    continue
                visited.add(current)
                for edge in self.edges:
                    if edge['target'] == current and edge['source'] not in visited:
                        next_level.append(edge['source'])
            to_visit = next_level
            current_depth += 1
        
        visited.discard(node_id)
        return visited

    def get_downstream(self, node_id, depth=None):
        visited = set()
        to_visit = [node_id]
        current_depth = 0
        
        while to_visit:
            if depth is not None and current_depth >= depth:
                break
            next_level = []
            for current in to_visit:
                if current in visited:
                    continue
                visited.add(current)
                for edge in self.edges:
                    if edge['source'] == current and edge['target'] not in visited:
                        next_level.append(edge['target'])
            to_visit = next_level
            current_depth += 1
        
        visited.discard(node_id)
        return visited

    def get_path(self, source, target):
        queue = [(source, [source])]
        visited = {source}
        
        while queue:
            current, path = queue.pop(0)
            if current == target:
                return path
            
            for edge in self.edges:
                if edge['source'] == current and edge['target'] not in visited:
                    visited.add(edge['target'])
                    queue.append((edge['target'], path + [edge['target']]))
        
        return []
