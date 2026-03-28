import ast
import re
from typing import Dict, List, Set, Tuple


class SparkColumnParser:
    def __init__(self):
        self.column_deps = {}
        self.udf_registry = {}

    def parse_notebook_cell(self, code: str, cell_id: int) -> Dict:
        try:
            tree = ast.parse(code)
            return self._extract_transformations(tree, cell_id)
        except SyntaxError:
            return {"error": "parse_failed", "cell": cell_id}

    def _extract_transformations(self, tree: ast.AST, cell_id: int) -> Dict:
        result = {"cell_id": cell_id, "operations": [], "udfs": []}
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                op = self._analyze_call(node)
                if op:
                    result["operations"].append(op)
            elif isinstance(node, ast.FunctionDef):
                if self._is_udf(node):
                    result["udfs"].append(self._extract_udf(node))
        return result

    def _analyze_call(self, node: ast.Call) -> Dict:
        func_name = self._get_func_name(node)
        if func_name in ['select', 'withColumn', 'withColumnRenamed']:
            return self._extract_column_mapping(func_name, node)
        elif func_name in ['join', 'union', 'unionByName']:
            return {"type": func_name, "columns": "all"}
        return None

    def _extract_column_mapping(self, func: str, node: ast.Call) -> Dict:
        mapping = {"type": func, "columns": []}
        for arg in node.args:
            cols = self._extract_columns(arg)
            mapping["columns"].extend(cols)
        return mapping

    def _extract_columns(self, node: ast.AST) -> List[Dict]:
        columns = []
        if isinstance(node, ast.Constant):
            columns.append({"name": node.value, "type": "literal"})
        elif isinstance(node, ast.Call):
            func = self._get_func_name(node)
            if func in ['col', 'column']:
                if node.args and isinstance(node.args[0], ast.Constant):
                    columns.append({"name": node.args[0].value, "type": "source"})
            elif func in ['sum', 'avg', 'count', 'max', 'min', 'concat']:
                deps = [self._extract_columns(arg) for arg in node.args]
                columns.append({"name": f"{func}_expr", "type": "derived", "deps": deps})
        return columns

    def _get_func_name(self, node: ast.Call) -> str:
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        elif isinstance(node.func, ast.Name):
            return node.func.id
        return ""

    def _is_udf(self, node: ast.FunctionDef) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and 'udf' in decorator.id:
                return True
            elif isinstance(decorator, ast.Call):
                name = self._get_func_name(decorator)
                if 'udf' in name:
                    return True
        return False

    def _extract_udf(self, node: ast.FunctionDef) -> Dict:
        params = [arg.arg for arg in node.args.args]
        return {"name": node.name, "params": params, "line": node.lineno}
