import re
import json
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, asdict

@dataclass
class ColumnLineage:
    source_column: str
    target_column: str
    transformation: str
    source_table: Optional[str] = None
    target_table: Optional[str] = None

class TransformationTracer:
    def __init__(self):
        self.lineages: List[ColumnLineage] = []
        self.graph: Dict[str, List[str]] = {}
    
    def parse_sql(self, sql: str, source_table: str = None, target_table: str = None) -> List[ColumnLineage]:
        lineages = []
        sql = sql.strip()
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return lineages
        select_clause = select_match.group(1)
        columns = [c.strip() for c in select_clause.split(',')]
        for col in columns:
            as_match = re.search(r'(.+?)\s+AS\s+(\w+)', col, re.IGNORECASE)
            if as_match:
                expr, alias = as_match.groups()
                lineages.append(ColumnLineage(
                    source_column=expr.strip(),
                    target_column=alias.strip(),
                    transformation=expr.strip(),
                    source_table=source_table,
                    target_table=target_table
                ))
            else:
                col_name = col.strip()
                if col_name != '*':
                    lineages.append(ColumnLineage(
                        source_column=col_name,
                        target_column=col_name,
                        transformation='direct',
                        source_table=source_table,
                        target_table=target_table
                    ))
        self.lineages.extend(lineages)
        return lineages
    
    def parse_python(self, code: str) -> List[ColumnLineage]:
        lineages = []
        lines = code.split('\n')
        for line in lines:
            assign_match = re.search(r'df\[[\'"](\w+)[\'"]\]\s*=\s*(.+)', line)
            if assign_match:
                col_name, expr = assign_match.groups()
                lineages.append(ColumnLineage(
                    source_column=expr.strip(),
                    target_column=col_name,
                    transformation=expr.strip()
                ))
        self.lineages.extend(lineages)
        return lineages
    
    def build_graph(self) -> Dict[str, Any]:
        nodes = set()
        edges = []
        for lin in self.lineages:
            src = f"{lin.source_table or 'unknown'}.{lin.source_column}"
            tgt = f"{lin.target_table or 'unknown'}.{lin.target_column}"
            nodes.add(src)
            nodes.add(tgt)
            edges.append({'from': src, 'to': tgt, 'transformation': lin.transformation})
        return {'nodes': list(nodes), 'edges': edges}
    
    def get_lineage_for_column(self, column: str) -> List[ColumnLineage]:
        return [lin for lin in self.lineages if lin.target_column == column]
    
    def export_json(self) -> str:
        return json.dumps([asdict(lin) for lin in self.lineages], indent=2)
