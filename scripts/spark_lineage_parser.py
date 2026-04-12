import ast
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field

@dataclass
class SparkOperation:
    op_type: str
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    transformation: str = ""
    line_number: int = 0

@dataclass
class UDFDefinition:
    name: str
    params: List[str]
    body: str
    return_cols: List[str] = field(default_factory=list)

class SparkLineageParser:
    def __init__(self):
        self.operations: List[SparkOperation] = []
        self.udfs: Dict[str, UDFDefinition] = {}
        self.dataframes: Dict[str, List[str]] = {}

    def parse_file(self, filepath: str) -> 'SparkLineageParser':
        with open(filepath, 'r') as f:
            code = f.read()
        return self.parse_code(code)

    def parse_code(self, code: str) -> 'SparkLineageParser':
        tree = ast.parse(code)
        self._extract_udfs(tree)
        self._extract_operations(tree)
        return self

    def _extract_udfs(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                if any(self._is_udf_decorator(d) for d in node.decorator_list):
                    params = [arg.arg for arg in node.args.args]
                    body = ast.unparse(node) if hasattr(ast, 'unparse') else ''
                    self.udfs[node.name] = UDFDefinition(node.name, params, body)

    def _is_udf_decorator(self, decorator: ast.AST) -> bool:
        if isinstance(decorator, ast.Name):
            return decorator.id in ['udf', 'pandas_udf']
        if isinstance(decorator, ast.Call):
            if isinstance(decorator.func, ast.Name):
                return decorator.func.id in ['udf', 'pandas_udf']
        return False

    def _extract_operations(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self._process_call(node)

    def _process_call(self, node: ast.Call):
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in ['select', 'withColumn', 'selectExpr']:
                op = self._parse_transform(node, method)
                if op:
                    self.operations.append(op)
            elif method == 'join':
                self.operations.append(SparkOperation('join', [], []))

    def _parse_transform(self, node: ast.Call, method: str) -> Optional[SparkOperation]:
        inputs, outputs = [], []
        if method == 'withColumn' and len(node.args) >= 2:
            col_name = self._extract_string(node.args[0])
            expr = ast.unparse(node.args[1]) if hasattr(ast, 'unparse') else ''
            inputs = self._extract_column_refs(expr)
            outputs = [col_name] if col_name else []
        elif method == 'select':
            for arg in node.args:
                col = self._extract_column_name(arg)
                if col:
                    outputs.append(col)
        return SparkOperation(method, inputs, outputs, line_number=node.lineno)

    def _extract_column_refs(self, expr: str) -> List[str]:
        cols = []
        patterns = [r"col\(['\"]([^'\"]+)['\"]\)", r"F\.col\(['\"]([^'\"]+)['\"]\)"]
        for pattern in patterns:
            cols.extend(re.findall(pattern, expr))
        return cols

    def _extract_column_name(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == 'alias' and node.args:
                return self._extract_string(node.args[0])
        return None

    def _extract_string(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Constant):
            return str(node.value)
        return None

    def get_column_lineage(self) -> List[Dict[str, str]]:
        lineage = []
        for op in self.operations:
            for inp in op.inputs:
                for out in op.outputs:
                    lineage.append({'source_col': inp, 'target_col': out, 'operation': op.op_type})
        return lineage
