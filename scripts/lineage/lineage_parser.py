import sqlparse
import re
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass

@dataclass
class ColumnLineage:
    target_column: str
    source_columns: List[Tuple[str, str]]
    transformation: str
    confidence: float

class ColumnLineageParser:
    def __init__(self):
        self.column_map = {}
        self.table_aliases = {}

    def parse_sql(self, sql: str) -> List[ColumnLineage]:
        parsed = sqlparse.parse(sql)[0]
        self._extract_table_aliases(parsed)
        return self._extract_column_lineage(parsed)

    def _extract_table_aliases(self, parsed):
        self.table_aliases = {}
        from_seen = False
        for token in parsed.tokens:
            if token.ttype is None and hasattr(token, 'tokens'):
                if 'FROM' in str(token).upper():
                    from_seen = True
                if from_seen:
                    match = re.search(r'(\w+)\s+(?:AS\s+)?(\w+)', str(token), re.I)
                    if match:
                        table, alias = match.groups()
                        self.table_aliases[alias] = table

    def _extract_column_lineage(self, parsed) -> List[ColumnLineage]:
        lineages = []
        select_cols = self._get_select_columns(parsed)
        for col_expr, col_name in select_cols:
            sources = self._identify_source_columns(col_expr)
            lineages.append(ColumnLineage(
                target_column=col_name,
                source_columns=sources,
                transformation=col_expr,
                confidence=0.9 if sources else 0.5
            ))
        return lineages

    def _get_select_columns(self, parsed) -> List[Tuple[str, str]]:
        columns = []
        in_select = False
        for token in parsed.tokens:
            if token.ttype == sqlparse.tokens.DML and token.value.upper() == 'SELECT':
                in_select = True
                continue
            if in_select and token.ttype is None:
                cols_str = str(token).split(',') if ',' in str(token) else [str(token)]
                for col in cols_str:
                    col = col.strip()
                    if ' AS ' in col.upper():
                        expr, name = re.split(r'\s+AS\s+', col, flags=re.I)
                        columns.append((expr.strip(), name.strip()))
                    else:
                        name = col.split('.')[-1] if '.' in col else col
                        columns.append((col, name.strip()))
                break
        return columns

    def _identify_source_columns(self, expression: str) -> List[Tuple[str, str]]:
        sources = []
        pattern = r'(\w+)\.(\w+)'
        matches = re.findall(pattern, expression)
        for alias, col in matches:
            table = self.table_aliases.get(alias, alias)
            sources.append((table, col))
        return sources
