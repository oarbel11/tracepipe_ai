import re
from typing import Dict, List, Any, Optional
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Token
from sqlparse.tokens import Keyword, DML

class ColumnLineageExtractor:
    def __init__(self, catalog_name: str):
        self.catalog_name = catalog_name

    def extract_lineage(self, sql: str, target_table: str) -> Dict[str, Any]:
        parsed = sqlparse.parse(sql)[0]
        lineage = {
            "target_table": target_table,
            "source_tables": self._extract_source_tables(parsed),
            "columns": self._extract_column_mappings(parsed)
        }
        return lineage

    def _extract_source_tables(self, parsed) -> List[str]:
        tables = []
        from_seen = False
        for token in parsed.tokens:
            if from_seen:
                if isinstance(token, Identifier):
                    tables.append(str(token.get_real_name()))
                    break
                elif token.ttype is None and not token.is_whitespace:
                    name = str(token).strip().split()[0]
                    if name.upper() not in ('WHERE', 'GROUP', 'ORDER', 'LIMIT'):
                        tables.append(name)
                        break
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
        return tables

    def _extract_column_mappings(self, parsed) -> Dict[str, Dict[str, Any]]:
        columns = {}
        select_seen = False
        for token in parsed.tokens:
            if token.ttype is DML and token.value.upper() == 'SELECT':
                select_seen = True
            elif select_seen and (isinstance(token, IdentifierList) or isinstance(token, Identifier) or isinstance(token, Function)):
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        self._process_identifier(identifier, columns)
                else:
                    self._process_identifier(token, columns)
                break
        return columns

    def _process_identifier(self, identifier, columns: Dict):
        alias = identifier.get_alias()
        real_name = identifier.get_real_name()
        token_str = str(identifier).strip()
        
        if isinstance(identifier, Function):
            func_name = identifier.get_name().upper()
            source_cols = self._extract_columns_from_expression(token_str)
            target_col = alias if alias else real_name
            columns[target_col] = {
                "source_columns": source_cols,
                "transformation_logic": token_str.split(' AS ')[0].strip() if ' AS ' in token_str.upper() else token_str,
                "transformation_type": func_name.lower()
            }
        elif alias:
            source_cols = self._extract_columns_from_expression(real_name if real_name else token_str.split(' AS ')[0].strip())
            columns[alias] = {
                "source_columns": source_cols,
                "transformation_logic": token_str.split(' AS ')[0].strip(),
                "transformation_type": self._classify_transformation(token_str)
            }
        else:
            col_name = real_name if real_name else token_str
            columns[col_name] = {
                "source_columns": [col_name],
                "transformation_logic": col_name,
                "transformation_type": "passthrough"
            }

    def _extract_columns_from_expression(self, expr: str) -> List[str]:
        cols = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
        return [c for c in cols if c.upper() not in ('CONCAT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'AS', 'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE')]

    def _classify_transformation(self, expr: str) -> str:
        expr_upper = expr.upper()
        if 'CONCAT' in expr_upper: return 'concat'
        if 'CASE' in expr_upper: return 'case'
        if 'CAST' in expr_upper: return 'cast'
        return 'expression'
