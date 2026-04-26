import ast
import re
from typing import Dict, List, Set, Any, Optional

class SparkLineageParser:
    def __init__(self):
        self.udfs = {}
        self.transformations = []
        self.column_lineage = {}
        self.dataframes = {}

    def parse(self, code: str) -> Dict[str, Any]:
        tree = ast.parse(code)
        self._extract_udfs(tree)
        self._extract_transformations(tree)
        return {
            'udfs': self.udfs,
            'transformations': self.transformations,
            'column_lineage': self.column_lineage
        }

    def parse_code(self, code: str) -> Dict[str, Any]:
        return self.parse(code)

    def _extract_udfs(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if self._is_udf_registration(node):
                    udf_info = self._parse_udf(node)
                    if udf_info:
                        self.udfs[udf_info['name']] = udf_info

    def _is_udf_registration(self, node: ast.Call) -> bool:
        if isinstance(node.func, ast.Attribute):
            return node.func.attr == 'udf'
        elif isinstance(node.func, ast.Name):
            return node.func.id == 'udf'
        return False

    def _parse_udf(self, node: ast.Call) -> Optional[Dict[str, Any]]:
        if not node.args:
            return None
        func_arg = node.args[0]
        func_name = None
        if isinstance(func_arg, ast.Name):
            func_name = func_arg.id
        elif isinstance(func_arg, ast.Lambda):
            func_name = 'lambda_udf'
        return {'name': func_name, 'type': 'udf'} if func_name else None

    def _extract_transformations(self, tree: ast.AST):
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                self._track_assignment(node)

    def _track_assignment(self, node: ast.Assign):
        if not node.targets:
            return
        target = node.targets[0]
        target_name = target.id if isinstance(target, ast.Name) else None
        if isinstance(node.value, ast.Call):
            self._track_dataframe_op(node.value, target_name)

    def _track_dataframe_op(self, node: ast.Call, target_name: Optional[str]):
        if isinstance(node.func, ast.Attribute):
            op = node.func.attr
            self.transformations.append({'operation': op, 'target': target_name})
            if target_name:
                self.dataframes[target_name] = {'op': op}
            if op in ['select', 'withColumn', 'withColumnRenamed']:
                self._track_column_lineage(node, op, target_name)

    def _track_column_lineage(self, node: ast.Call, op: str, target: Optional[str]):
        source_df = self._get_source_df(node.func)
        if op == 'select' and node.args:
            for arg in node.args:
                col_name = self._extract_column_name(arg)
                if col_name and target:
                    qual_target = f"{target}.{col_name}"
                    qual_source = f"{source_df}.{col_name}" if source_df else col_name
                    self.column_lineage[qual_target] = [qual_source]
        elif op == 'withColumn' and len(node.args) >= 2:
            col_name = self._extract_string_value(node.args[0])
            if col_name and target:
                qual_col = f"{target}.{col_name}"
                self.column_lineage[qual_col] = self._extract_dependencies(node.args[1], source_df)

    def _get_source_df(self, func_node: ast.Attribute) -> Optional[str]:
        if isinstance(func_node.value, ast.Name):
            return func_node.value.id
        return None

    def _extract_column_name(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Constant):
            return str(node.value) if isinstance(node.value, str) else None
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'col':
            return self._extract_string_value(node.args[0]) if node.args else None
        return None

    def _extract_string_value(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, (ast.Str, ast.Constant)):
            return node.s if isinstance(node, ast.Str) else str(node.value)
        return None

    def _extract_dependencies(self, node: ast.AST, source_df: Optional[str]) -> List[str]:
        deps = []
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'col':
                col = self._extract_string_value(n.args[0]) if n.args else None
                if col:
                    deps.append(f"{source_df}.{col}" if source_df else col)
        return deps
