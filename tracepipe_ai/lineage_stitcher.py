from typing import Dict, List
from tracepipe_ai.lineage_integration import LineageGraph, LineageEdge


class LineageStitcher:
    def __init__(self):
        self.unified_graph = LineageGraph()

    def stitch(self, graphs: List[LineageGraph],
               mappings: List[Dict]) -> LineageGraph:
        for graph in graphs:
            for node_id, node in graph.nodes.items():
                self.unified_graph.add_node(node)
            for edge in graph.edges:
                self.unified_graph.add_edge(edge)

        for mapping in mappings:
            source = mapping.get("source")
            target = mapping.get("target")
            if source and target:
                edge = LineageEdge(source, target, "cross_system")
                self.unified_graph.add_edge(edge)

        return self.unified_graph

    def get_column_lineage(self, column_id: str) -> Dict:
        upstream = self._trace_upstream(column_id, set())
        downstream = self._trace_downstream(column_id, set())
        return {
            "column": column_id,
            "upstream": list(upstream),
            "downstream": list(downstream)
        }

    def _trace_upstream(self, node_id: str, visited: set) -> set:
        if node_id in visited:
            return set()
        visited.add(node_id)
        upstream = set(self.unified_graph.get_upstream(node_id))
        for parent in list(upstream):
            upstream.update(self._trace_upstream(parent, visited))
        return upstream

    def _trace_downstream(self, node_id: str, visited: set) -> set:
        if node_id in visited:
            return set()
        visited.add(node_id)
        downstream = set(self.unified_graph.get_downstream(node_id))
        for child in list(downstream):
            downstream.update(self._trace_downstream(child, visited))
        return downstream

    def query_lineage(self, query: Dict) -> Dict:
        entity_id = query.get("entity_id")
        if not entity_id:
            return {"error": "entity_id required"}

        if entity_id not in self.unified_graph.nodes:
            return {"error": "entity not found"}

        node = self.unified_graph.nodes[entity_id]
        return {
            "entity_id": entity_id,
            "system": node.system,
            "type": node.entity_type,
            "name": node.name,
            "upstream": self.unified_graph.get_upstream(entity_id),
            "downstream": self.unified_graph.get_downstream(entity_id)
        }
