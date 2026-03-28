import networkx as nx
from typing import Dict, List, Set, Tuple
import json


class ColumnLineageGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.tables = {}
        self.column_metadata = {}

    def add_table(self, table_name: str, columns: List[str]):
        self.tables[table_name] = columns
        for col in columns:
            node_id = f"{table_name}.{col}"
            self.graph.add_node(node_id, table=table_name, column=col, type="base")

    def add_transformation(self, source_cols: List[str], target_col: str,
                          operation: str, cell_id: int):
        target_node = target_col
        if "." not in target_col:
            target_node = f"temp.{target_col}"
        
        self.graph.add_node(target_node, column=target_col,
                           operation=operation, cell=cell_id, type="derived")
        
        for src in source_cols:
            src_node = src if "." in src else f"temp.{src}"
            if not self.graph.has_node(src_node):
                self.graph.add_node(src_node, column=src, type="intermediate")
            self.graph.add_edge(src_node, target_node,
                              operation=operation, cell=cell_id)

    def get_upstream_lineage(self, column: str, max_depth: int = 10) -> Dict:
        node = column if "." in column else f"temp.{column}"
        if not self.graph.has_node(node):
            return {"error": "column_not_found", "column": column}
        
        upstream = []
        visited = set()
        self._dfs_upstream(node, upstream, visited, 0, max_depth)
        return {"column": column, "upstream": upstream, "count": len(upstream)}

    def _dfs_upstream(self, node: str, result: List, visited: Set,
                     depth: int, max_depth: int):
        if depth >= max_depth or node in visited:
            return
        visited.add(node)
        
        for pred in self.graph.predecessors(node):
            edge_data = self.graph.get_edge_data(pred, node)
            result.append({
                "from": pred,
                "to": node,
                "operation": edge_data.get("operation", "unknown"),
                "cell": edge_data.get("cell", -1),
                "depth": depth
            })
            self._dfs_upstream(pred, result, visited, depth + 1, max_depth)

    def get_downstream_impact(self, column: str) -> Dict:
        node = column if "." in column else f"temp.{column}"
        if not self.graph.has_node(node):
            return {"error": "column_not_found"}
        
        downstream = list(nx.descendants(self.graph, node))
        return {"column": column, "impacted": downstream, "count": len(downstream)}

    def export_visualization(self) -> Dict:
        nodes = []
        edges = []
        
        for node, data in self.graph.nodes(data=True):
            nodes.append({"id": node, **data})
        
        for src, tgt, data in self.graph.edges(data=True):
            edges.append({"source": src, "target": tgt, **data})
        
        return {"nodes": nodes, "edges": edges}

    def to_json(self) -> str:
        return json.dumps(self.export_visualization(), indent=2)
