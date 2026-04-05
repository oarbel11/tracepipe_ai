"""Parse SQL and code to extract column-level lineage."""
import re
from typing import Dict, List, Set
from dataclasses import dataclass, field


@dataclass
class ColumnLineage:
    """Column lineage information."""
    target_column: str
    source_columns: List[str] = field(default_factory=list)
    transformation: str = ""
    source_table: str = ""


class ColumnLineageParser:
    """Parse SQL to extract column-level lineage."""

    def parse_sql(self, sql: str) -> List[ColumnLineage]:
        """Parse SQL and extract column lineage."""
        lineages = []
        sql_upper = sql.upper()

        if 'SELECT' in sql_upper:
            select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)
                columns = [c.strip() for c in select_clause.split(',')]

                for col in columns:
                    if ' AS ' in col.upper():
                        parts = re.split(r'\s+AS\s+', col, flags=re.IGNORECASE)
                        expr = parts[0].strip()
                        alias = parts[1].strip()
                        sources = self._extract_column_refs(expr)
                        lineages.append(ColumnLineage(
                            target_column=alias,
                            source_columns=sources,
                            transformation=expr
                        ))
                    else:
                        col_name = col.strip()
                        if col_name != '*':
                            sources = self._extract_column_refs(col_name)
                            lineages.append(ColumnLineage(
                                target_column=col_name,
                                source_columns=sources if sources else [col_name],
                                transformation=col_name
                            ))

        return lineages

    def _extract_column_refs(self, expr: str) -> List[str]:
        """Extract column references from expression."""
        matches = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
        keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'}
        return [m for m in matches if m.upper() not in keywords]

    def parse_dataframe_code(self, code: str) -> List[ColumnLineage]:
        """Parse DataFrame transformation code."""
        lineages = []
        withcolumn_pattern = r'\.withColumn\s*\(\s*["\']([^"\'\']+)["\']\s*,\s*(.+?)\)'
        matches = re.finditer(withcolumn_pattern, code)

        for match in matches:
            target = match.group(1)
            expr = match.group(2)
            sources = self._extract_column_refs(expr)
            lineages.append(ColumnLineage(
                target_column=target,
                source_columns=sources,
                transformation=expr
            ))

        return lineages
