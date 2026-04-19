import ast
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field
import networkx as nx

@dataclass
class ColumnNode:
    table: str
    column: str
    transformation: Optional[str] = None
    
    def __hash__(self):
        return hash((self.table, self.column))
    
    def __eq__(self, other):
        return self.table == other.table and self.column == other.column

@dataclass
class ColumnLineage:
    graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    udf_definitions: Dict[str, ast.FunctionDef] = field(default_factory=dict)
    dataframe_ops: List[Dict] = field(default_factory=list)

class ColumnLineageAnalyzer:
    def __init__(self):
        self.lineage = ColumnLineage()
        self.current_notebook = None
        
    def load_notebook(self, filepath: str):
        with open(filepath, 'r') as f:
            self.current_notebook = f.read()
        return self
    
    def analyze(self) -> ColumnLineage:
        if not self.current_notebook:
            return self.lineage
        tree = ast.parse(self.current_notebook)
        self._extract_udfs(tree)
        self._analyze_dataframe_ops(tree)
        return self.lineage
    
    def _extract_udfs(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if self._is_udf_decorator(decorator):
                        self.lineage.udf_definitions[node.name] = node
                        self._analyze_udf_body(node)
    
    def _is_udf_decorator(self, decorator) -> bool:
        if isinstance(decorator, ast.Name):
            return decorator.id in ['udf', 'pandas_udf']
        if isinstance(decorator, ast.Call):
            if isinstance(decorator.func, ast.Name):
                return decorator.func.id in ['udf', 'pandas_udf']
        return False
    
    def _analyze_udf_body(self, func_def: ast.FunctionDef):
        params = [arg.arg for arg in func_def.args.args]
        for node in ast.walk(func_def):
            if isinstance(node, ast.Return) and node.value:
                self._track_expression_deps(node.value, params)
    
    def _analyze_dataframe_ops(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self._process_df_call(node)
    
    def _process_df_call(self, call_node: ast.Call):
        if isinstance(call_node.func, ast.Attribute):
            method = call_node.func.attr
            if method == 'select':
                self._handle_select(call_node)
            elif method == 'withColumn':
                self._handle_with_column(call_node)
            elif method in ['groupBy', 'agg']:
                self._handle_aggregation(call_node)
    
    def _handle_select(self, node: ast.Call):
        cols = self._extract_column_names(node.args)
        self.lineage.dataframe_ops.append({'op': 'select', 'columns': cols})
    
    def _handle_with_column(self, node: ast.Call):
        if len(node.args) >= 2:
            new_col = self._get_string_value(node.args[0])
            expr = node.args[1]
            deps = self._extract_dependencies(expr)
            self.lineage.dataframe_ops.append({
                'op': 'withColumn', 'column': new_col, 'dependencies': deps
            })
    
    def _handle_aggregation(self, node: ast.Call):
        self.lineage.dataframe_ops.append({'op': 'aggregation', 'node': node})
    
    def _extract_column_names(self, args: List) -> List[str]:
        cols = []
        for arg in args:
            if isinstance(arg, ast.Constant):
                cols.append(arg.value)
            elif isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name):
                if arg.func.id == 'col' and arg.args:
                    cols.append(self._get_string_value(arg.args[0]))
        return cols
    
    def _extract_dependencies(self, expr) -> Set[str]:
        deps = set()
        for node in ast.walk(expr):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == 'col':
                    if node.args:
                        deps.add(self._get_string_value(node.args[0]))
        return deps
    
    def _track_expression_deps(self, expr, params: List[str]):
        pass
    
    def _get_string_value(self, node) -> Optional[str]:
        if isinstance(node, ast.Constant):
            return str(node.value)
        return None
    
    def get_column_lineage(self, table: str, column: str) -> List[ColumnNode]:
        lineage_path = []
        for op in reversed(self.lineage.dataframe_ops):
            if op['op'] == 'withColumn' and op['column'] == column:
                for dep in op.get('dependencies', []):
                    lineage_path.append(ColumnNode(table='source', column=dep))
        return lineage_path
