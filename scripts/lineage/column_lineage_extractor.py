import re
from typing import Dict, List, Set, Optional


class ColumnLineageExtractor:
    def __init__(self, catalog: str = "main", schema: str = "default"):
        self.catalog = catalog
        self.schema = schema
        self.lineage_graph = {}

    def extract_from_sql(self, sql: str, target_table: str) -> Dict:
        """Extract column lineage from SQL query."""
        columns = self._parse_select_columns(sql)
        sources = self._parse_source_tables(sql)
        
        lineage = {
            "target_table": target_table,
            "columns": {},
            "sources": sources
        }
        
        for col_name, expression in columns.items():
            source_cols = self._extract_source_columns(expression)
            lineage["columns"][col_name] = {
                "expression": expression,
                "source_columns": source_cols,
                "transformation_type": self._classify_transformation(expression)
            }
        
        return lineage

    def _parse_select_columns(self, sql: str) -> Dict[str, str]:
        """Parse SELECT columns from SQL."""
        sql_upper = sql.upper()
        select_idx = sql_upper.find("SELECT")
        from_idx = sql_upper.find("FROM")
        
        if select_idx == -1 or from_idx == -1:
            return {}
        
        select_clause = sql[select_idx + 6:from_idx].strip()
        columns = {}
        
        for item in select_clause.split(","):
            item = item.strip()
            if " AS " in item.upper():
                expr, alias = re.split(r"\s+AS\s+", item, flags=re.IGNORECASE)
                columns[alias.strip()] = expr.strip()
            else:
                col_name = item.split(".")[-1].strip()
                columns[col_name] = item
        
        return columns

    def _parse_source_tables(self, sql: str) -> List[str]:
        """Extract source tables from SQL."""
        tables = []
        from_pattern = r"FROM\s+([\w.]+)"
        join_pattern = r"JOIN\s+([\w.]+)"
        
        tables.extend(re.findall(from_pattern, sql, re.IGNORECASE))
        tables.extend(re.findall(join_pattern, sql, re.IGNORECASE))
        
        return list(set(tables))

    def _extract_source_columns(self, expression: str) -> List[str]:
        """Extract source columns from expression."""
        column_pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b"
        matches = re.findall(column_pattern, expression)
        return list(set(matches))

    def _classify_transformation(self, expression: str) -> str:
        """Classify transformation type."""
        expr_upper = expression.upper()
        if any(func in expr_upper for func in ["SUM", "AVG", "COUNT", "MAX", "MIN"]):
            return "aggregation"
        elif any(func in expr_upper for func in ["CONCAT", "SUBSTRING", "UPPER", "LOWER"]):
            return "string_manipulation"
        elif any(op in expression for op in ["+", "-", "*", "/"]):
            return "calculation"
        else:
            return "direct"
