import networkx as nx
from typing import Dict, List, Set, Optional
from dataclasses import dataclass

@dataclass
class ColumnNode:
    table: str
    column: str
    operation: str = ""

    def __hash__(self):
        return hash((self.table, self.column))

    def __eq__(self, other):
        if not isinstance(other, ColumnNode):
            return False
        return self.table == other.table and self.column == other.column

class LineageExtractor:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.column_mappings: Dict[str, Set[str]] = {}

    def add_transformation(self, source_table: str, source_col: str,
                          target_table: str, target_col: str, operation: str = ""):
        src = ColumnNode(source_table, source_col, operation)
        tgt = ColumnNode(target_table, target_col, operation)
        self.graph.add_edge(src, tgt, operation=operation)
        key = f"{target_table}.{target_col}"
        if key not in self.column_mappings:
            self.column_mappings[key] = set()
        self.column_mappings[key].add(f"{source_table}.{source_col}")

    def get_upstream_columns(self, table: str, column: str) -> List[ColumnNode]:
        target = ColumnNode(table, column)
        if target not in self.graph:
            return []
        upstream = []
        for node in nx.ancestors(self.graph, target):
            upstream.append(node)
        return upstream

    def get_downstream_columns(self, table: str, column: str) -> List[ColumnNode]:
        source = ColumnNode(table, column)
        if source not in self.graph:
            return []
        downstream = []
        for node in nx.descendants(self.graph, source):
            downstream.append(node)
        return downstream

    def get_lineage_path(self, source_table: str, source_col: str,
                        target_table: str, target_col: str) -> List[List[ColumnNode]]:
        src = ColumnNode(source_table, source_col)
        tgt = ColumnNode(target_table, target_col)
        if src not in self.graph or tgt not in self.graph:
            return []
        try:
            paths = nx.all_simple_paths(self.graph, src, tgt)
            return [list(path) for path in paths]
        except nx.NetworkXNoPath:
            return []

    def get_column_impact(self, table: str, column: str) -> Dict:
        upstream = self.get_upstream_columns(table, column)
        downstream = self.get_downstream_columns(table, column)
        return {
            'column': f"{table}.{column}",
            'upstream_count': len(upstream),
            'downstream_count': len(downstream),
            'upstream': [f"{n.table}.{n.column}" for n in upstream],
            'downstream': [f"{n.table}.{n.column}" for n in downstream]
        }

    def export_lineage(self) -> List[Dict]:
        lineage = []
        for src, tgt, data in self.graph.edges(data=True):
            lineage.append({
                'source_table': src.table,
                'source_column': src.column,
                'target_table': tgt.table,
                'target_column': tgt.column,
                'operation': data.get('operation', '')
            })
        return lineage
