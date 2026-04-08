from typing import Dict, List, Any, Optional

class ImpactAnalyzer:
    def __init__(self):
        self.lineage_graph = {}

    def analyze_column_impact(self, column: str, table: str, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        self.lineage_graph = lineage_data
        
        downstream = self._find_downstream_dependencies(column, table)
        upstream = self._find_upstream_dependencies(column, table)
        
        affected_objects = self._collect_affected_objects(downstream)
        
        impact_score = self._calculate_impact_score(downstream, upstream)
        
        return {
            "column": column,
            "table": table,
            "downstream_dependencies": downstream,
            "upstream_dependencies": upstream,
            "affected_objects": affected_objects,
            "impact_score": impact_score
        }

    def _find_downstream_dependencies(self, column: str, table: str, visited: Optional[set] = None) -> List[Dict[str, Any]]:
        if visited is None:
            visited = set()
        
        key = f"{table}.{column}"
        if key in visited:
            return []
        visited.add(key)
        
        dependencies = []
        for target_table, lineage in self.lineage_graph.items():
            if table in lineage.get("source_tables", []):
                for col_name, col_info in lineage.get("columns", {}).items():
                    if column in col_info.get("source_columns", []):
                        dep = {
                            "table": target_table,
                            "column": col_name,
                            "transformation": col_info.get("transformation_type", "unknown")
                        }
                        dependencies.append(dep)
                        dependencies.extend(self._find_downstream_dependencies(col_name, target_table, visited))
        
        return dependencies

    def _find_upstream_dependencies(self, column: str, table: str) -> List[Dict[str, Any]]:
        dependencies = []
        lineage = self.lineage_graph.get(table, {})
        col_info = lineage.get("columns", {}).get(column, {})
        
        for source_col in col_info.get("source_columns", []):
            for source_table in lineage.get("source_tables", []):
                dependencies.append({
                    "table": source_table,
                    "column": source_col,
                    "transformation": col_info.get("transformation_type", "unknown")
                })
        
        return dependencies

    def _collect_affected_objects(self, downstream: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        tables = list(set([dep["table"] for dep in downstream]))
        return {
            "tables": tables,
            "reports": [],
            "dashboards": []
        }

    def _calculate_impact_score(self, downstream: List[Dict[str, Any]], upstream: List[Dict[str, Any]]) -> int:
        return len(downstream) * 10 + len(upstream) * 5
