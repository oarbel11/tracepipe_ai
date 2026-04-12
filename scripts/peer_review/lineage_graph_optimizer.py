from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
import math

@dataclass
class GraphCluster:
    cluster_id: str
    nodes: Set[str]
    edges: List[Tuple[str, str]]
    depth: int

@dataclass
class ViewportWindow:
    center_node: str
    max_depth: int = 2
    max_nodes: int = 100

class LineageGraphOptimizer:
    def __init__(self, max_viewport_nodes: int = 100):
        self.max_viewport_nodes = max_viewport_nodes
        self.clusters: Dict[str, GraphCluster] = {}

    def build_clusters(self, lineage: Dict[str, List[str]], 
                      cluster_size: int = 50) -> Dict[str, GraphCluster]:
        visited = set()
        clusters = {}
        cluster_id = 0
        
        for root in lineage.keys():
            if root not in visited:
                nodes, edges = self._bfs_cluster(lineage, root, 
                                                cluster_size, visited)
                if nodes:
                    clusters[f"cluster_{cluster_id}"] = GraphCluster(
                        cluster_id=f"cluster_{cluster_id}",
                        nodes=nodes,
                        edges=edges,
                        depth=self._calculate_depth(lineage, root)
                    )
                    cluster_id += 1
        
        self.clusters = clusters
        return clusters

    def _bfs_cluster(self, lineage: Dict[str, List[str]], start: str, 
                    max_size: int, visited: Set[str]) -> Tuple[Set[str], List[Tuple[str, str]]]:
        nodes = set()
        edges = []
        queue = [start]
        
        while queue and len(nodes) < max_size:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            nodes.add(node)
            
            for child in lineage.get(node, []):
                edges.append((node, child))
                if child not in visited and len(nodes) < max_size:
                    queue.append(child)
        
        return nodes, edges

    def get_viewport_subgraph(self, lineage: Dict[str, List[str]], 
                             viewport: ViewportWindow) -> Dict:
        nodes = set([viewport.center_node])
        edges = []
        
        upstream = self._traverse_upstream(lineage, viewport.center_node, 
                                          viewport.max_depth)
        downstream = self._traverse_downstream(lineage, viewport.center_node, 
                                              viewport.max_depth)
        
        nodes.update(upstream)
        nodes.update(downstream)
        
        if len(nodes) > self.max_viewport_nodes:
            nodes = self._prune_nodes(nodes, viewport.center_node, 
                                     self.max_viewport_nodes)
        
        for node in nodes:
            for child in lineage.get(node, []):
                if child in nodes:
                    edges.append({"source": node, "target": child})
        
        return {
            "nodes": [{"id": n} for n in nodes],
            "edges": edges,
            "metadata": {"total_nodes": len(nodes), "total_edges": len(edges)}
        }

    def _traverse_upstream(self, lineage: Dict[str, List[str]], 
                          node: str, max_depth: int) -> Set[str]:
        reverse_lineage = self._reverse_graph(lineage)
        return self._traverse_downstream(reverse_lineage, node, max_depth)

    def _traverse_downstream(self, lineage: Dict[str, List[str]], 
                            node: str, max_depth: int) -> Set[str]:
        nodes = set()
        queue = [(node, 0)]
        
        while queue:
            current, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for child in lineage.get(current, []):
                nodes.add(child)
                queue.append((child, depth + 1))
        
        return nodes

    def _reverse_graph(self, lineage: Dict[str, List[str]]) -> Dict[str, List[str]]:
        reverse = {}
        for parent, children in lineage.items():
            for child in children:
                reverse.setdefault(child, []).append(parent)
        return reverse

    def _prune_nodes(self, nodes: Set[str], center: str, max_nodes: int) -> Set[str]:
        return set(list(nodes)[:max_nodes])

    def _calculate_depth(self, lineage: Dict[str, List[str]], root: str) -> int:
        max_depth = 0
        queue = [(root, 0)]
        visited = set()
        
        while queue:
            node, depth = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            max_depth = max(max_depth, depth)
            for child in lineage.get(node, []):
                queue.append((child, depth + 1))
        
        return max_depth
