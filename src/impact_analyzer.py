from typing import Dict, List, Any

class ImpactAnalyzer:
    def __init__(self):
        self.lineage_graph = {}
    
    def add_lineage(self, table: str, lineage: Dict[str, Any]):
        if table not in self.lineage_graph:
            self.lineage_graph[table] = {}
        
        for col, info in lineage.get("columns", {}).items():
            self.lineage_graph[table][col] = {
                "source_columns": info.get("source_columns", []),
                "downstream": []
            }
    
    def analyze_impact(self, table: str, column: str) -> Dict[str, Any]:
        impacted = {"tables": [], "columns": []}
        
        if table not in self.lineage_graph or column not in self.lineage_graph[table]:
            return impacted
        
        # Find all downstream dependencies
        visited = set()
        self._find_downstream(table, column, impacted, visited)
        
        return impacted
    
    def _find_downstream(self, table: str, column: str, impacted: Dict, visited: set):
        key = f"{table}.{column}"
        if key in visited:
            return
        visited.add(key)
        
        if table in self.lineage_graph and column in self.lineage_graph[table]:
            for downstream in self.lineage_graph[table][column].get("downstream", []):
                impacted["columns"].append(downstream)
                down_table, down_col = downstream.split(".")
                if down_table not in impacted["tables"]:
                    impacted["tables"].append(down_table)
                self._find_downstream(down_table, down_col, impacted, visited)
