import ast
from typing import List, Dict, Any, Optional

class SparkLineageParser:
    def __init__(self):
        self.dataframes = {}
        self.udfs = {}
        self.operations = []

    def parse_code(self, code: str) -> Dict[str, Any]:
        tree = ast.parse(code)
        self._analyze_tree(tree)
        return {
            'dataframes': self.dataframes,
            'udfs': self.udfs,
            'operations': self.operations
        }

    def _analyze_tree(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                self._process_assignment(node)
            elif isinstance(node, ast.FunctionDef):
                self._process_function(node)

    def _process_assignment(self, node: ast.Assign):
        if not node.targets or not isinstance(node.value, ast.Call):
            return
        target = self._get_name(node.targets[0])
        if not target:
            return
        call_info = self._analyze_call(node.value)
        if call_info:
            self.dataframes[target] = call_info
            self.operations.append({
                'target': target,
                'operation': call_info['operation'],
                'source': call_info.get('source'),
                'columns': call_info.get('columns', [])
            })

    def _analyze_call(self, node: ast.Call) -> Optional[Dict[str, Any]]:
        if isinstance(node.func, ast.Attribute):
            obj_name = self._get_name(node.func.value)
            method = node.func.attr
            if method in ['select', 'withColumn', 'filter', 'groupBy']:
                cols = [self._extract_column(arg) for arg in node.args]
                return {'operation': method, 'source': obj_name, 'columns': cols}
        return None

    def _extract_column(self, node: ast.AST) -> str:
        if isinstance(node, ast.Constant):
            return str(node.value)
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == 'col' and node.args:
                return self._extract_column(node.args[0])
        return 'unknown'

    def _process_function(self, node: ast.FunctionDef):
        if any(dec.id == 'udf' for dec in node.decorator_list if isinstance(dec, ast.Name)):
            self.udfs[node.name] = self._analyze_udf(node)

    def _analyze_udf(self, node: ast.FunctionDef) -> Dict[str, Any]:
        params = [arg.arg for arg in node.args.args]
        return {'name': node.name, 'params': params}

    def _get_name(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Name):
            return node.id
        return None
