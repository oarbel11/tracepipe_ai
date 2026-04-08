import sqlparse
from typing import Dict, List, Any

class ColumnLineageExtractor:
    def extract_lineage(self, sql: str) -> Dict[str, Any]:
        lineage = {"columns": {}, "transformations": []}
        
        # Parse SQL
        parsed = sqlparse.parse(sql)
        if not parsed:
            return lineage
        
        stmt = parsed[0]
        
        # Extract SELECT columns
        select_seen = False
        from_seen = False
        source_table = None
        
        for token in stmt.tokens:
            if token.ttype is sqlparse.tokens.Keyword.DML and token.value.upper() == 'SELECT':
                select_seen = True
            elif select_seen and not from_seen:
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                    from_seen = True
                elif isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        self._process_column(identifier, lineage, source_table)
                elif isinstance(token, sqlparse.sql.Identifier):
                    self._process_column(token, lineage, source_table)
            elif from_seen and isinstance(token, sqlparse.sql.Identifier):
                source_table = token.get_real_name()
        
        return lineage
    
    def _process_column(self, identifier, lineage, source_table):
        col_alias = identifier.get_alias()
        col_expr = identifier.value
        
        if col_alias:
            col_name = col_alias
            # Remove alias from expression
            expr_part = col_expr.split(' AS ')[0].strip()
        else:
            col_name = identifier.get_real_name() or str(identifier).strip()
            expr_part = col_expr.strip()
        
        # Check if it's a transformation (contains function or operation)
        is_transformation = any([
            '(' in expr_part,
            '||' in expr_part,
            '+' in expr_part,
            '-' in expr_part and not expr_part.startswith('-'),
            '*' in expr_part,
            '/' in expr_part
        ])
        
        if is_transformation:
            lineage["columns"][col_name] = {
                "source_columns": self._extract_source_cols(expr_part),
                "transformation_type": "expression",
                "expression": expr_part
            }
        else:
            # Pass-through column
            lineage["columns"][col_name] = {
                "source_columns": [col_name],
                "transformation_type": "passthrough"
            }
    
    def _extract_source_cols(self, expr: str) -> List[str]:
        cols = []
        tokens = sqlparse.parse(expr)[0].flatten()
        for token in tokens:
            if token.ttype is None and token.value not in ['(', ')', ',', '||', ' ']:
                if token.value.strip() and not token.value.strip().startswith("'"):
                    cols.append(token.value.strip())
        return cols
