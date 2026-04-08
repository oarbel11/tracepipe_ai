import re
from typing import Dict, List, Any

class ColumnLineageExtractor:
    def __init__(self, workspace_client=None):
        self.workspace_client = workspace_client
    
    def extract_lineage(self, table_name: str, sql_query: str = None) -> Dict:
        if not sql_query:
            return {"table": table_name, "columns": {}}
        
        columns = {}
        lines = sql_query.strip().split('\n')
        in_select = False
        
        for line in lines:
            line = line.strip()
            if line.upper().startswith('SELECT'):
                in_select = True
                line = line[6:].strip()
            elif line.upper().startswith('FROM'):
                in_select = False
                break
            
            if in_select and line:
                line = line.rstrip(',')
                parts = re.split(r'\s+[Aa][Ss]\s+', line, maxsplit=1)
                
                if len(parts) == 2:
                    expr = parts[0].strip()
                    alias = parts[1].strip()
                    sources = self._extract_source_columns(expr)
                    trans_type = self._classify_transformation(expr, sources)
                    
                    columns[alias] = {
                        "source_columns": sources,
                        "transformation_type": trans_type,
                        "transformation_logic": expr
                    }
        
        return {"table": table_name, "columns": columns}
    
    def _extract_source_columns(self, expression: str) -> List[str]:
        expr_upper = expression.upper()
        if 'CONCAT' in expr_upper or '||' in expression:
            matches = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expression)
            return [m for m in matches if m.upper() not in 
                   ['CONCAT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'AS']]
        elif 'CASE' in expr_upper:
            matches = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expression)
            return [m for m in matches if m.upper() not in 
                   ['CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR']]
        elif 'CAST' in expr_upper:
            match = re.search(r'CAST\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)', 
                            expression, re.IGNORECASE)
            return [match.group(1)] if match else []
        else:
            match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)$', expression.strip())
            return [match.group(1)] if match else []
    
    def _classify_transformation(self, logic: str, sources: List[str]) -> str:
        logic_upper = logic.upper()
        if 'CONCAT' in logic_upper or '||' in logic:
            return 'concat'
        elif 'CASE' in logic_upper:
            return 'conditional'
        elif 'CAST' in logic_upper:
            return 'cast'
        elif any(agg in logic_upper for agg in ['SUM', 'AVG', 'COUNT', 'MAX']):
            return 'aggregation'
        elif len(sources) == 1:
            return 'direct'
        return 'expression'
