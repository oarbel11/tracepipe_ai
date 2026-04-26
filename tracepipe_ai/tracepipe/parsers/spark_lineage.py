"""Spark lineage parser with UDF support."""

import ast
from typing import Dict, List, Set, Optional


class SparkLineageParser:
    def __init__(self):
        self.lineage = {}
        self.udfs = {}
        self.current_df = None

    def parse_code(self, code: str) -> Dict:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                if any(isinstance(d, ast.Name) and d.id == 'udf' 
                       for d in node.decorator_list):
                    self._parse_udf(node)
            elif isinstance(node, ast.Assign):
                self._parse_assignment(node)
        return self.lineage

    def _parse_udf(self, node: ast.FunctionDef):
        deps = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Name) and n.id in node.args.args[0].arg:
                deps.add(n.id)
        self.udfs[node.name] = {'dependencies': list(deps)}

    def _parse_assignment(self, node: ast.Assign):
        if not isinstance(node.value, ast.Call):
            return
        target = node.targets[0].id if node.targets else None
        if not target:
            return
        
        if isinstance(node.value.func, ast.Attribute):
            method = node.value.func.attr
            if method in ['select', 'withColumn', 'drop']:
                self._track_transformation(target, node.value, method)

    def _track_transformation(self, target: str, call: ast.Call, method: str):
        base = self._get_base_df(call.func)
        if not base:
            return
        
        if method == 'select':
            cols = [self._extract_col(arg) for arg in call.args]
            self.lineage[target] = {c: [f"{base}.{c}"] for c in cols if c}
        elif method == 'withColumn':
            col_name = self._get_string_arg(call.args[0])
            deps = self._extract_dependencies(call.args[1], base)
            self.lineage[target] = self.lineage.get(base, {}).copy()
            if col_name:
                self.lineage[target][col_name] = deps
        elif method == 'drop':
            dropped = [self._extract_col(arg) for arg in call.args]
            self.lineage[target] = {k: v for k, v in 
                                    self.lineage.get(base, {}).items() 
                                    if k not in dropped}

    def _get_base_df(self, node: ast.Attribute) -> Optional[str]:
        if isinstance(node.value, ast.Name):
            return node.value.id
        return None

    def _extract_col(self, node) -> Optional[str]:
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == 'col' and node.args:
                return self._extract_col(node.args[0])
        return None

    def _get_string_arg(self, node) -> Optional[str]:
        return node.value if isinstance(node, ast.Constant) else None

    def _extract_dependencies(self, node, base: str) -> List[str]:
        deps = []
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name):
                if n.func.id == 'col' and n.args:
                    col = self._extract_col(n.args[0])
                    if col:
                        deps.append(f"{base}.{col}")
        return deps or []
