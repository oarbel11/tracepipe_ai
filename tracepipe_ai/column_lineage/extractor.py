import re
from typing import Dict, List, Any

class ColumnLineageExtractor:
    def __init__(self):
        self.lineage_graph = {}
    
    def extract_lineage(self, sql: str, target_table: str) -> Dict[str, Any]:
        lineage = {
            "target_table": target_table,
            "columns": {}
        }
        
        select_pattern = r'SELECT\s+(.+?)\s+FROM'
        match = re.search(select_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return lineage
        
        select_clause = match.group(1)
        column_expressions = [c.strip() for c in select_clause.split(',')]
        
        for expr in column_expressions:
            if ' AS ' in expr.upper():
                parts = re.split(r'\s+AS\s+', expr, flags=re.IGNORECASE)
                expression = parts[0].strip()
                alias = parts[1].strip()
            else:
                expression = expr
                alias = expr
            
            source_cols = self._extract_source_columns(expression)
            trans_type = self._classify_transformation(expression)
            
            lineage["columns"][alias] = {
                "source_columns": source_cols,
                "transformation_type": trans_type,
                "expression": expression
            }
        
        return lineage
    
    def _extract_source_columns(self, expression: str) -> List[str]:
        column_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        matches = re.findall(column_pattern, expression)
        keywords = {'CONCAT', 'CAST', 'AS', 'INT', 'STRING', 'SUM', 'AVG'}
        return [m for m in matches if m.upper() not in keywords]
    
    def _classify_transformation(self, expression: str) -> str:
        expr_upper = expression.upper()
        
        if 'CONCAT' in expr_upper:
            return 'string_manipulation'
        elif 'CAST' in expr_upper:
            return 'type_conversion'
        elif any(agg in expr_upper for agg in ['SUM', 'AVG', 'COUNT', 'MAX']):
            return 'aggregation'
        elif any(op in expression for op in ['+', '-', '*', '/']):
            return 'arithmetic'
        else:
            return 'direct'
