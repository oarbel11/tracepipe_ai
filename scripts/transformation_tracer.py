import re
import networkx as nx
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass

@dataclass
class ColumnLineage:
    source_column: str
    target_column: str
    transformation: str
    source_table: str = ""
    target_table: str = ""

class TransformationTracer:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.column_lineages = []

    def parse_sql(self, sql: str) -> List[ColumnLineage]:
        lineages = []
        sql = sql.strip()
        
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        match = re.search(select_pattern, sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return lineages
        
        select_clause = match.group(1)
        columns = [c.strip() for c in select_clause.split(',')]
        
        for col in columns:
            if ' AS ' in col.upper():
                parts = re.split(r'\s+AS\s+', col, flags=re.IGNORECASE)
                expr = parts[0].strip()
                alias = parts[1].strip()
                source_cols = self._extract_column_refs(expr)
                for src in source_cols:
                    lineages.append(ColumnLineage(
                        source_column=src,
                        target_column=alias,
                        transformation=expr
                    ))
            else:
                clean_col = col.strip()
                if clean_col != '*':
                    lineages.append(ColumnLineage(
                        source_column=clean_col,
                        target_column=clean_col,
                        transformation="direct"
                    ))
        
        return lineages

    def _extract_column_refs(self, expr: str) -> List[str]:
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        matches = re.findall(pattern, expr)
        functions = {'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'UPPER', 'LOWER'}
        return [m for m in matches if m.upper() not in functions]

    def parse_python(self, code: str) -> List[ColumnLineage]:
        lineages = []
        pattern = r'df\["(\w+)"\]\s*=\s*(.+)'
        matches = re.findall(pattern, code)
        
        for target, expr in matches:
            source_cols = self._extract_column_refs(expr)
            for src in source_cols:
                lineages.append(ColumnLineage(
                    source_column=src,
                    target_column=target,
                    transformation=expr.strip()
                ))
        
        return lineages

    def build_graph(self, lineages: List[ColumnLineage]):
        for lineage in lineages:
            src_node = f"{lineage.source_table}.{lineage.source_column}" if lineage.source_table else lineage.source_column
            tgt_node = f"{lineage.target_table}.{lineage.target_column}" if lineage.target_table else lineage.target_column
            self.graph.add_edge(src_node, tgt_node, transformation=lineage.transformation)
        self.column_lineages.extend(lineages)
