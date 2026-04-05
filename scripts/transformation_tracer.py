import sqlparse
import re
import networkx as nx
from typing import Dict, List, Set, Tuple

class ColumnLineage:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.transformations = {}
    
    def add_column(self, table: str, column: str, metadata: dict = None):
        node_id = f"{table}.{column}"
        self.graph.add_node(node_id, table=table, column=column, 
                           metadata=metadata or {})
    
    def add_transformation(self, source_cols: List[str], target_col: str,
                          operation: str, expression: str = ""):
        self.graph.add_node(target_col, type='derived')
        for src in source_cols:
            self.graph.add_edge(src, target_col, operation=operation,
                               expression=expression)
        self.transformations[target_col] = {
            'sources': source_cols, 'operation': operation,
            'expression': expression
        }
    
    def get_column_lineage(self, column: str) -> Dict:
        if column not in self.graph:
            return {}
        ancestors = nx.ancestors(self.graph, column)
        return {'column': column, 'depends_on': list(ancestors),
                'transformation': self.transformations.get(column, {})}

class TransformationTracer:
    def __init__(self):
        self.udf_registry = {}
    
    def trace_transformations(self, sql_code: str, 
                             source_table: str = None) -> ColumnLineage:
        lineage = ColumnLineage()
        parsed = sqlparse.parse(sql_code)
        
        for stmt in parsed:
            if stmt.get_type() == 'SELECT':
                self._parse_select(stmt, lineage, source_table)
        
        return lineage
    
    def _parse_select(self, stmt, lineage: ColumnLineage, source_table: str):
        sql = str(stmt)
        from_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        table = from_match.group(1) if from_match else source_table
        
        select_items = self._extract_select_items(sql)
        
        for alias, expr in select_items:
            sources = self._extract_column_refs(expr, table)
            operation = self._classify_operation(expr)
            target = f"{table}_derived.{alias}" if alias else expr
            
            if sources:
                lineage.add_transformation(sources, target, operation, expr)
            else:
                lineage.add_column(table or 'unknown', alias or expr)
    
    def _extract_select_items(self, sql: str) -> List[Tuple[str, str]]:
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, 
                                re.IGNORECASE | re.DOTALL)
        if not select_match:
            return []
        
        items = []
        select_clause = select_match.group(1)
        for item in select_clause.split(','):
            item = item.strip()
            as_match = re.search(r'(.+)\s+AS\s+(\w+)', item, re.IGNORECASE)
            if as_match:
                items.append((as_match.group(2), as_match.group(1).strip()))
            else:
                items.append((item, item))
        return items
    
    def _extract_column_refs(self, expr: str, table: str) -> List[str]:
        cols = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
        keywords = {'SELECT', 'FROM', 'WHERE', 'AS', 'AND', 'OR'}
        return [f"{table}.{c}" for c in cols 
                if c.upper() not in keywords and not c.isdigit()]
    
    def _classify_operation(self, expr: str) -> str:
        if '*' in expr or '/' in expr:
            return 'arithmetic'
        if 'CASE' in expr.upper():
            return 'conditional'
        if any(f in expr.upper() for f in ['SUM', 'COUNT', 'AVG', 'MAX']):
            return 'aggregation'
        if '+' in expr or '-' in expr:
            return 'arithmetic'
        return 'projection'
