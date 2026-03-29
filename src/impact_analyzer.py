from typing import Dict, List, Set
from dataclasses import dataclass, field

@dataclass
class ImpactNode:
    table: str
    column: str
    node_type: str = "table"
    dependencies: Set[str] = field(default_factory=set)

class ImpactAnalyzer:
    def __init__(self):
        self.lineage_graph = {}
    
    def build_graph(self, lineage_data: List[Dict]):
        for lineage in lineage_data:
            target = lineage["target_table"]
            if target not in self.lineage_graph:
                self.lineage_graph[target] = {}
            
            for col_name, col_info in lineage["columns"].items():
                self.lineage_graph[target][col_name] = {
                    "sources": col_info["source_columns"],
                    "transformation_type": col_info["transformation_type"]
                }
    
    def analyze_impact(self, table: str, column: str) -> Dict:
        downstream = self._find_downstream(table, column)
        upstream = self._find_upstream(table, column)
        
        return {
            "source": {"table": table, "column": column},
            "downstream_impact": list(downstream),
            "upstream_dependencies": list(upstream),
            "total_affected": len(downstream)
        }
    
    def _find_downstream(self, table: str, column: str, visited=None) -> Set[str]:
        if visited is None:
            visited = set()
        
        affected = set()
        key = f"{table}.{column}"
        
        if key in visited:
            return affected
        visited.add(key)
        
        for tbl, cols in self.lineage_graph.items():
            for col, info in cols.items():
                if table in str(info["sources"]) or column in info["sources"]:
                    affected.add(f"{tbl}.{col}")
                    affected.update(self._find_downstream(tbl, col, visited))
        
        return affected
    
    def _find_upstream(self, table: str, column: str, visited=None) -> Set[str]:
        if visited is None:
            visited = set()
        
        dependencies = set()
        key = f"{table}.{column}"
        
        if key in visited:
            return dependencies
        visited.add(key)
        
        if table in self.lineage_graph and column in self.lineage_graph[table]:
            sources = self.lineage_graph[table][column]["sources"]
            for src in sources:
                dependencies.add(src)
        
        return dependencies
