import ast
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field
import json

@dataclass
class ColumnLineage:
    source_columns: Set[str] = field(default_factory=set)
    target_column: str = ""
    transformation_type: str = ""
    udf_name: Optional[str] = None
    expression: str = ""

@dataclass
class TableLineage:
    table_name: str
    columns: Dict[str, ColumnLineage] = field(default_factory=dict)

class UDFAnalyzer(ast.NodeVisitor):
    def __init__(self):
        self.column_deps = {}
        self.current_func = None
        self.param_names = []

    def visit_FunctionDef(self, node):
        self.current_func = node.name
        self.param_names = [arg.arg for arg in node.args.args]
        self.column_deps[node.name] = set(self.param_names)
        self.generic_visit(node)

    def visit_Name(self, node):
        if self.current_func and node.id in self.param_names:
            self.column_deps[self.current_func].add(node.id)
        self.generic_visit(node)

    def analyze(self, code: str) -> Dict[str, Set[str]]:
        tree = ast.parse(code)
        self.visit(tree)
        return self.column_deps

class ColumnLineageTracker:
    def __init__(self):
        self.lineages: Dict[str, TableLineage] = {}
        self.udf_analyzer = UDFAnalyzer()
        self.dataframe_ops = {}

    def analyze_udf(self, udf_code: str, udf_name: str) -> Dict[str, Set[str]]:
        return self.udf_analyzer.analyze(udf_code)

    def track_withcolumn(self, table: str, new_col: str, expr: str, source_cols: List[str]):
        if table not in self.lineages:
            self.lineages[table] = TableLineage(table)
        lineage = ColumnLineage(
            source_columns=set(source_cols),
            target_column=new_col,
            transformation_type="withColumn",
            expression=expr
        )
        self.lineages[table].columns[new_col] = lineage

    def track_select(self, source_table: str, target_table: str, column_mapping: Dict[str, List[str]]):
        if target_table not in self.lineages:
            self.lineages[target_table] = TableLineage(target_table)
        for target_col, source_cols in column_mapping.items():
            lineage = ColumnLineage(
                source_columns=set(source_cols),
                target_column=target_col,
                transformation_type="select"
            )
            self.lineages[target_table].columns[target_col] = lineage

    def get_column_flow(self, table: str, column: str) -> List[str]:
        if table not in self.lineages or column not in self.lineages[table].columns:
            return []
        lineage = self.lineages[table].columns[column]
        return list(lineage.source_columns)

    def export_to_openlineage(self, output_path: str):
        data = {t: {"columns": {c: {"sources": list(l.source_columns), "type": l.transformation_type} 
                for c, l in tl.columns.items()}} for t, tl in self.lineages.items()}
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

def track_transformation(tracker: ColumnLineageTracker):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator
