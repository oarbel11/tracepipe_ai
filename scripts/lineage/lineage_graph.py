"""Build and manage column lineage graphs."""

from typing import Dict, List, Set, Tuple
import json


class ColumnLineageGraph:
    """Represents column-level lineage as a directed graph."""

    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: List[Tuple[str, str, Dict]] = []

    def add_column(self, table: str, column: str, metadata: Dict = None) -> str:
        """Add a column node to the graph."""
        node_id = f"{table}.{column}"
        self.nodes[node_id] = {
            "table": table,
            "column": column,
            "metadata": metadata or {}
        }
        return node_id

    def add_transformation(self, source: str, target: str, 
                          operation: str, expression: str = "") -> None:
        """Add a transformation edge between columns."""
        self.edges.append((source, target, {
            "operation": operation,
            "expression": expression
        }))

    def get_upstream_columns(self, column_id: str) -> List[str]:
        """Get all upstream columns for a given column."""
        upstream = []
        for source, target, _ in self.edges:
            if target == column_id:
                upstream.append(source)
        return upstream

    def get_downstream_columns(self, column_id: str) -> List[str]:
        """Get all downstream columns for a given column."""
        downstream = []
        for source, target, _ in self.edges:
            if source == column_id:
                downstream.append(target)
        return downstream

    def build_from_mappings(self, mappings: List[Dict], 
                           source_table: str, target_table: str) -> None:
        """Build graph from column mappings."""
        for mapping in mappings:
            target_col = self.add_column(target_table, mapping["target"])
            
            for source_col_name in mapping["sources"]:
                source_col = self.add_column(source_table, source_col_name)
                self.add_transformation(
                    source_col, target_col,
                    mapping["operation"],
                    mapping.get("expression", "")
                )

    def to_dict(self) -> Dict:
        """Export graph as dictionary."""
        return {
            "nodes": self.nodes,
            "edges": [
                {"source": s, "target": t, "metadata": m}
                for s, t, m in self.edges
            ]
        }

    def to_json(self) -> str:
        """Export graph as JSON string."""
        return json.dumps(self.to_dict(), indent=2)
