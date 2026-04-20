import re
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from scripts.column_lineage_tracker import ColumnLineageTracker, ColumnLineage

@dataclass
class SparkLineageNode:
    table_name: str
    operation: str
    columns: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    column_lineage: Dict[str, ColumnLineage] = field(default_factory=dict)

class SparkLineageParser:
    def __init__(self):
        self.nodes: Dict[str, SparkLineageNode] = {}
        self.column_tracker = ColumnLineageTracker()
        self.df_registry: Dict[str, str] = {}

    def parse_dataframe_op(self, code: str, df_var: str) -> Optional[SparkLineageNode]:
        select_match = re.search(rf'{df_var}\.select\((.+?)\)', code)
        if select_match:
            cols = [c.strip().strip('"\' ') for c in select_match.group(1).split(',')]
            return SparkLineageNode(table_name=df_var, operation='select', columns=cols)
        
        withcol_match = re.search(rf'{df_var}\.withColumn\(["\'](.+?)["\'],\s*(.+?)\)', code)
        if withcol_match:
            new_col = withcol_match.group(1)
            expr = withcol_match.group(2)
            source_cols = self._extract_columns_from_expr(expr)
            return SparkLineageNode(table_name=df_var, operation='withColumn', 
                                  columns=[new_col], dependencies=source_cols)
        
        groupby_match = re.search(rf'{df_var}\.groupBy\((.+?)\)\.agg\((.+?)\)', code)
        if groupby_match:
            group_cols = [c.strip().strip('"\' ') for c in groupby_match.group(1).split(',')]
            agg_expr = groupby_match.group(2)
            return SparkLineageNode(table_name=df_var, operation='groupBy', 
                                  columns=group_cols, dependencies=group_cols)
        return None

    def _extract_columns_from_expr(self, expr: str) -> List[str]:
        col_pattern = r'col\(["\'](.+?)["\']\)|F\.col\(["\'](.+?)["\']\)'
        matches = re.findall(col_pattern, expr)
        return [m[0] or m[1] for m in matches if m[0] or m[1]]

    def parse_notebook(self, notebook_content: str) -> Dict[str, SparkLineageNode]:
        lines = notebook_content.split('\n')
        for line in lines:
            df_match = re.match(r'(\w+)\s*=\s*(\w+)\.', line)
            if df_match:
                new_df, source_df = df_match.groups()
                node = self.parse_dataframe_op(line, source_df)
                if node:
                    self.nodes[new_df] = node
                    self.df_registry[new_df] = source_df
        return self.nodes

    def get_lineage_graph(self) -> Dict[str, List[str]]:
        graph = {}
        for name, node in self.nodes.items():
            graph[name] = node.dependencies if node.dependencies else []
        return graph

    def get_column_lineage(self, table: str, column: str) -> List[str]:
        return self.column_tracker.get_column_flow(table, column)
