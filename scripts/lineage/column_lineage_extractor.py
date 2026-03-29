import sqlparse
from sqlparse.sql import Identifier, Function, Where, Comparison
from typing import Dict, List, Set, Tuple
import re


class ColumnLineageExtractor:
    def __init__(self):
        self.lineage_graph = {}
        self.transformations = {}
        self.udf_registry = {}

    def extract_from_sql(self, sql: str, table_name: str) -> Dict:
        parsed = sqlparse.parse(sql)[0]
        columns = self._extract_columns(parsed)
        dependencies = self._extract_dependencies(parsed, columns)
        transformations = self._extract_transformations(parsed, columns)
        
        return {
            'table': table_name,
            'columns': columns,
            'dependencies': dependencies,
            'transformations': transformations
        }

    def _extract_columns(self, parsed) -> List[str]:
        columns = []
        for token in parsed.tokens:
            if token.ttype is None and 'SELECT' in str(token).upper():
                select_part = str(token)
                for col in re.findall(r'(\w+)\s+AS\s+\w+|,\s*(\w+)', select_part, re.IGNORECASE):
                    col_name = col[0] or col[1]
                    if col_name and col_name.upper() not in ['SELECT', 'FROM', 'WHERE']:
                        columns.append(col_name)
        return columns

    def _extract_dependencies(self, parsed, columns: List[str]) -> Dict[str, List[str]]:
        deps = {}
        sql_str = str(parsed)
        
        for col in columns:
            pattern = rf'{col}\s*=\s*([\w\s\.\+\-\*/\(\)]+)'
            matches = re.findall(pattern, sql_str, re.IGNORECASE)
            if matches:
                source_cols = re.findall(r'\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b', matches[0])
                deps[col] = [f"{table}.{col}" for table, col in source_cols]
        return deps

    def _extract_transformations(self, parsed, columns: List[str]) -> Dict[str, Dict]:
        trans = {}
        sql_str = str(parsed)
        
        for col in columns:
            trans[col] = {
                'type': self._detect_transformation_type(sql_str, col),
                'expression': self._extract_expression(sql_str, col),
                'udfs': self._extract_udfs(sql_str, col)
            }
        return trans

    def _detect_transformation_type(self, sql: str, col: str) -> str:
        pattern = rf'{col}\s*[=,]\s*([^,\n]+)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if not match:
            return 'direct'
        expr = match.group(1).lower()
        if any(f in expr for f in ['sum(', 'avg(', 'count(', 'max(', 'min(']):
            return 'aggregation'
        elif any(op in expr for op in ['+', '-', '*', '/']):
            return 'arithmetic'
        elif 'case' in expr:
            return 'conditional'
        elif 'cast' in expr or '::' in expr:
            return 'type_conversion'
        return 'function'

    def _extract_expression(self, sql: str, col: str) -> str:
        pattern = rf'{col}\s*[=,]\s*([^,\n]+)'
        match = re.search(pattern, sql, re.IGNORECASE)
        return match.group(1).strip() if match else ''

    def _extract_udfs(self, sql: str, col: str) -> List[str]:
        pattern = rf'{col}\s*[=,]\s*([^,\n]+)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if not match:
            return []
        expr = match.group(1)
        udf_pattern = r'\b([a-z_]\w*)\s*\('
        potential_udfs = re.findall(udf_pattern, expr, re.IGNORECASE)
        builtins = {'sum', 'avg', 'count', 'max', 'min', 'cast', 'coalesce', 'case'}
        return [u for u in potential_udfs if u.lower() not in builtins]
