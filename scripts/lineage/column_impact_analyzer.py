from typing import Dict, List, Set
from databricks.sdk import WorkspaceClient


class ColumnImpactAnalyzer:
    def __init__(self, workspace_client: WorkspaceClient, extractor):
        self.client = workspace_client
        self.extractor = extractor
        self.impact_cache = {}

    def analyze_column_change(self, table: str, column: str) -> Dict:
        """Analyze impact of changing a column."""
        key = f"{table}.{column}"
        if key in self.impact_cache:
            return self.impact_cache[key]
        
        impact = {
            "affected_tables": [],
            "affected_columns": [],
            "affected_queries": [],
            "affected_dashboards": [],
            "risk_level": "LOW"
        }
        
        downstream = self._find_downstream_dependencies(table, column)
        impact["affected_tables"] = list(downstream["tables"])
        impact["affected_columns"] = list(downstream["columns"])
        
        if len(impact["affected_tables"]) > 10:
            impact["risk_level"] = "HIGH"
        elif len(impact["affected_tables"]) > 5:
            impact["risk_level"] = "MEDIUM"
        
        self.impact_cache[key] = impact
        return impact

    def _find_downstream_dependencies(self, table: str, column: str) -> Dict:
        """Find all downstream dependencies."""
        visited_tables = set()
        visited_columns = set()
        to_process = [(table, column)]
        
        while to_process:
            curr_table, curr_col = to_process.pop(0)
            key = f"{curr_table}.{curr_col}"
            
            if key in visited_columns:
                continue
            
            visited_columns.add(key)
            visited_tables.add(curr_table)
        
        return {
            "tables": visited_tables,
            "columns": visited_columns
        }

    def get_impact_report(self, table: str, column: str) -> str:
        """Generate human-readable impact report."""
        impact = self.analyze_column_change(table, column)
        
        report = f"Impact Analysis for {table}.{column}\n"
        report += f"Risk Level: {impact['risk_level']}\n"
        report += f"Affected Tables: {len(impact['affected_tables'])}\n"
        report += f"Affected Columns: {len(impact['affected_columns'])}\n"
        
        return report
