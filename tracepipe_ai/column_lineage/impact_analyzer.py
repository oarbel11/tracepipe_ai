from typing import Dict, List, Set, Any

class ImpactAnalyzer:
    def __init__(self):
        self.lineage_graph = {}
    
    def add_lineage(self, lineage: Dict[str, Any]):
        target = lineage["target_table"]
        if target not in self.lineage_graph:
            self.lineage_graph[target] = {}
        
        for col_name, col_info in lineage["columns"].items():
            self.lineage_graph[target][col_name] = col_info
    
    def analyze_impact(self, table: str, column: str) -> Dict[str, Any]:
        impacted = self._find_downstream_impact(table, column)
        
        return {
            "source": {"table": table, "column": column},
            "impacted_objects": impacted,
            "total_impacted": len(impacted)
        }
    
    def _find_downstream_impact(self, table: str, column: str) -> List[Dict]:
        impacted = []
        visited = set()
        
        def traverse(curr_table, curr_col, depth=0):
            key = f"{curr_table}.{curr_col}"
            if key in visited or depth > 10:
                return
            visited.add(key)
            
            for tbl, cols in self.lineage_graph.items():
                for col_name, col_info in cols.items():
                    if curr_col in col_info.get("source_columns", []):
                        impacted.append({
                            "table": tbl,
                            "column": col_name,
                            "transformation": col_info.get("transformation_type")
                        })
                        traverse(tbl, col_name, depth + 1)
        
        traverse(table, column)
        return impacted
