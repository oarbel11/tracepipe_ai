import re
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field

@dataclass
class ColumnLineage:
    source_columns: List[str] = field(default_factory=list)
    transformation_type: str = "direct"
    transformation_logic: Optional[str] = None
    dependencies: Set[str] = field(default_factory=set)

class ColumnLineageExtractor:
    def __init__(self):
        self.lineage_cache = {}
    
    def extract_lineage(self, sql: str, target_table: str) -> Dict:
        columns = self._parse_columns(sql)
        lineage = {
            "target_table": target_table,
            "columns": {}
        }
        
        for col_name, col_info in columns.items():
            lineage["columns"][col_name] = {
                "source_columns": col_info["sources"],
                "transformation_type": col_info["type"],
                "transformation_logic": col_info["logic"]
            }
        
        return lineage
    
    def _parse_columns(self, sql: str) -> Dict:
        columns = {}
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        
        if not select_match:
            return columns
        
        select_clause = select_match.group(1)
        col_exprs = self._split_columns(select_clause)
        
        for expr in col_exprs:
            col_name, sources, trans_type, logic = self._analyze_expression(expr)
            if col_name:
                columns[col_name] = {
                    "sources": sources,
                    "type": trans_type,
                    "logic": logic
                }
        
        return columns
    
    def _split_columns(self, select_clause: str) -> List[str]:
        columns = []
        current = ""
        paren_depth = 0
        
        for char in select_clause:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                columns.append(current.strip())
                current = ""
                continue
            current += char
        
        if current.strip():
            columns.append(current.strip())
        
        return columns
    
    def _analyze_expression(self, expr: str):
        alias_match = re.search(r'\s+(?:AS\s+)?(\w+)\s*$', expr, re.IGNORECASE)
        col_name = alias_match.group(1) if alias_match else None
        logic = expr.split(' AS ')[0].strip() if ' AS ' in expr.upper() else expr.strip()
        sources = re.findall(r'\b([a-zA-Z_]\w*)\b', logic)
        sources = [s for s in sources if s.upper() not in ['CONCAT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'AS', 'SUM', 'COUNT', 'AVG', 'MAX', 'MIN']]
        trans_type = self._classify_transformation(logic, sources)
        return col_name, sources, trans_type, logic
    
    def _classify_transformation(self, logic: str, sources: List[str]) -> str:
        logic_upper = logic.upper()
        if re.search(r'\bCONCAT\s*\(', logic_upper):
            return "string_manipulation"
        if re.search(r'\bCASE\s+WHEN', logic_upper):
            return "conditional"
        if re.search(r'\bCAST\s*\(', logic_upper):
            return "type_conversion"
        if re.search(r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(', logic_upper):
            return "aggregation"
        if len(sources) == 1 and logic.strip() == sources[0]:
            return "direct"
        return "direct"
