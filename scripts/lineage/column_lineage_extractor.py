import re
from typing import Dict, List, Set, Optional
from databricks.sdk import WorkspaceClient


class ColumnLineageExtractor:
    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client
        self.lineage_cache = {}

    def extract_from_sql(self, sql: str, source_table: str) -> Dict[str, List[str]]:
        """Extract column lineage from SQL query."""
        lineage = {}
        sql_upper = sql.upper()
        
        select_match = re.search(r"SELECT\s+(.+?)\s+FROM", sql_upper, re.DOTALL)
        if not select_match:
            return lineage
        
        select_clause = select_match.group(1)
        columns = [c.strip() for c in select_clause.split(",")]
        
        for col in columns:
            if "AS" in col:
                parts = col.split("AS")
                target = parts[1].strip()
                source_expr = parts[0].strip()
            else:
                target = col.strip()
                source_expr = col.strip()
            
            source_cols = self._extract_column_references(source_expr)
            lineage[target] = source_cols
        
        return lineage

    def _extract_column_references(self, expression: str) -> List[str]:
        """Extract column references from expression."""
        pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b"
        matches = re.findall(pattern, expression)
        
        keywords = {"SUM", "AVG", "COUNT", "MAX", "MIN", "CASE", "WHEN", 
                    "THEN", "ELSE", "END", "AND", "OR", "NOT", "AS"}
        return [m for m in matches if m.upper() not in keywords]

    def extract_from_dataframe(self, df_code: str) -> Dict[str, List[str]]:
        """Extract lineage from DataFrame transformations."""
        lineage = {}
        
        select_pattern = r"\.select\(([^)]+)\)"
        matches = re.findall(select_pattern, df_code)
        
        for match in matches:
            cols = [c.strip().strip('"').strip("'") for c in match.split(",")]
            for col in cols:
                lineage[col] = [col]
        
        withcolumn_pattern = r"\.withColumn\(['\"]([^'\"]+)['\"],\s*(.+?)\)"
        matches = re.findall(withcolumn_pattern, df_code)
        
        for col_name, expr in matches:
            source_cols = self._extract_column_references(expr)
            lineage[col_name] = source_cols
        
        return lineage

    def get_column_lineage(self, table_name: str, column_name: str) -> Dict:
        """Get full lineage for a specific column."""
        key = f"{table_name}.{column_name}"
        if key in self.lineage_cache:
            return self.lineage_cache[key]
        
        lineage_data = {
            "column": column_name,
            "table": table_name,
            "upstream": [],
            "transformations": []
        }
        
        self.lineage_cache[key] = lineage_data
        return lineage_data
