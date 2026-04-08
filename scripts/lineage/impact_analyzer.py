from typing import Dict, List, Set


class ImpactAnalyzer:
    def __init__(self):
        self.lineage_graph = {}
        self.downstream_cache = {}

    def add_lineage(self, table: str, lineage: Dict):
        """Add lineage information to the graph."""
        self.lineage_graph[table] = lineage
        self.downstream_cache = {}

    def analyze_column_impact(self, table: str, column: str) -> Dict:
        """Analyze impact of changes to a specific column."""
        affected = self._find_downstream(table, column)
        
        return {
            "source": f"{table}.{column}",
            "affected_tables": list(affected["tables"]),
            "affected_columns": list(affected["columns"]),
            "impact_depth": affected["depth"],
            "transformation_chain": affected["chain"]
        }

    def _find_downstream(self, table: str, column: str, 
                         visited: Set = None, depth: int = 0) -> Dict:
        """Recursively find downstream dependencies."""
        if visited is None:
            visited = set()
        
        key = f"{table}.{column}"
        if key in visited:
            return {"tables": set(), "columns": set(), "depth": depth, "chain": []}
        
        visited.add(key)
        affected_tables = set()
        affected_columns = set()
        chain = []
        max_depth = depth
        
        for downstream_table, lineage in self.lineage_graph.items():
            if "columns" not in lineage:
                continue
            
            for col_name, col_info in lineage["columns"].items():
                source_cols = col_info.get("source_columns", [])
                
                for src_col in source_cols:
                    if src_col.startswith(table + "."):
                        src_col_name = src_col.split(".")[-1]
                        if src_col_name == column:
                            affected_tables.add(downstream_table)
                            affected_columns.add(f"{downstream_table}.{col_name}")
                            chain.append({
                                "from": key,
                                "to": f"{downstream_table}.{col_name}",
                                "transformation": col_info.get("transformation_type")
                            })
                            
                            downstream = self._find_downstream(
                                downstream_table, col_name, visited, depth + 1
                            )
                            affected_tables.update(downstream["tables"])
                            affected_columns.update(downstream["columns"])
                            chain.extend(downstream["chain"])
                            max_depth = max(max_depth, downstream["depth"])
        
        return {
            "tables": affected_tables,
            "columns": affected_columns,
            "depth": max_depth,
            "chain": chain
        }
