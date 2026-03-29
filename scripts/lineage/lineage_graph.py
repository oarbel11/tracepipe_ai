from typing import Dict, List, Set, Optional

class LineageNode:
    def __init__(self, node_id: str, node_type: str, platform: str, 
                 metadata: Optional[Dict] = None):
        self.node_id = node_id
        self.node_type = node_type
        self.platform = platform
        self.metadata = metadata or {}

class LineageGraphBuilder:
    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[tuple] = []

    def add_node(self, node_id: str, node_type: str, platform: str, 
                 metadata: Optional[Dict] = None) -> None:
        node = LineageNode(node_id, node_type, platform, metadata)
        self.nodes[node_id] = node

    def add_edge(self, source_id: str, target_id: str, 
                 edge_type: str = 'data_flow') -> None:
        if source_id in self.nodes and target_id in self.nodes:
            self.edges.append((source_id, target_id, edge_type))

    def get_upstream(self, node_id: str) -> List[str]:
        return [src for src, tgt, _ in self.edges if tgt == node_id]

    def get_downstream(self, node_id: str) -> List[str]:
        return [tgt for src, tgt, _ in self.edges if src == node_id]

    def get_all_paths(self, source_id: str, target_id: str) -> List[List[str]]:
        if source_id not in self.nodes or target_id not in self.nodes:
            return []
        
        paths = []
        visited = set()
        self._dfs_paths(source_id, target_id, [source_id], visited, paths)
        return paths

    def _dfs_paths(self, current: str, target: str, path: List[str], 
                   visited: Set[str], paths: List[List[str]]) -> None:
        if current == target:
            paths.append(path.copy())
            return
        
        visited.add(current)
        for next_node in self.get_downstream(current):
            if next_node not in visited:
                path.append(next_node)
                self._dfs_paths(next_node, target, path, visited, paths)
                path.pop()
        visited.remove(current)

    def get_lineage_summary(self) -> Dict:
        return {
            'node_count': len(self.nodes),
            'edge_count': len(self.edges),
            'platforms': list(set(n.platform for n in self.nodes.values())),
            'node_types': list(set(n.node_type for n in self.nodes.values()))
        }

    def export_graph(self) -> Dict:
        return {
            'nodes': [{**vars(n)} for n in self.nodes.values()],
            'edges': [{'source': s, 'target': t, 'type': e} 
                      for s, t, e in self.edges]
        }
