from typing import Dict, List, Set

class ColumnLineageTracker:
    def __init__(self):
        self.lineage_graph = {}
        self.table_aliases = {}

    def add_lineage(self, target: str, sources: List[str]):
        """Add lineage relationship between target and source columns"""
        if target not in self.lineage_graph:
            self.lineage_graph[target] = set()
        self.lineage_graph[target].update(sources)

    def track_transformation(self, output_col: str, input_cols: List[str], 
                           output_table: str = None, input_table: str = None):
        """Track column transformation with table context"""
        if output_table:
            qualified_output = f"{output_table}.{output_col}"
        else:
            qualified_output = output_col

        sources = []
        for col in input_cols:
            if input_table:
                sources.append(f"{input_table}.{col}")
            else:
                sources.append(col)

        self.add_lineage(qualified_output, sources)

    def get_lineage(self, column: str) -> Set[str]:
        """Get all source columns for a target column"""
        if column in self.lineage_graph:
            return self.lineage_graph[column]
        return set()

    def get_full_lineage(self) -> Dict[str, List[str]]:
        """Get complete lineage graph"""
        return {k: list(v) for k, v in self.lineage_graph.items()}

    def get_transitive_lineage(self, column: str) -> Set[str]:
        """Get all transitive dependencies for a column"""
        result = set()
        to_process = [column]
        visited = set()

        while to_process:
            current = to_process.pop()
            if current in visited:
                continue
            visited.add(current)

            if current in self.lineage_graph:
                sources = self.lineage_graph[current]
                result.update(sources)
                to_process.extend(sources)

        return result

    def register_table_alias(self, alias: str, table_name: str):
        """Register table alias for qualified column names"""
        self.table_aliases[alias] = table_name
