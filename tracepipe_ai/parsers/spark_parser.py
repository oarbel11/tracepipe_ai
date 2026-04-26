"""Spark lineage parser with UDF and complex transformation support."""
import ast
import re
from typing import Dict, List, Set


class SparkLineageParser:
    """Parse Spark code to extract column-level lineage."""

    def __init__(self):
        self.udfs = {}
        self.lineage = {}

    def parse_code(self, code: str) -> Dict[str, List[str]]:
        """Parse Spark code and extract column lineage."""
        self.udfs = {}
        self.lineage = {}
        tree = ast.parse(code)
        self._extract_udfs(tree)
        self._extract_lineage(tree)
        return self.lineage

    def _extract_udfs(self, tree: ast.AST):
        """Extract UDF definitions from AST."""
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                if any(isinstance(d, ast.Name) and d.id == 'udf' 
                       for d in ast.walk(node)):
                    self.udfs[node.name] = self._get_udf_deps(node)

    def _get_udf_deps(self, func_node: ast.FunctionDef) -> Set[str]:
        """Get column dependencies from UDF function."""
        deps = set()
        for node in ast.walk(func_node):
            if isinstance(node, ast.Subscript):
                if isinstance(node.value, ast.Name):
                    if isinstance(node.slice, ast.Constant):
                        deps.add(node.slice.value)
        return deps

    def _extract_lineage(self, tree: ast.AST):
        """Extract lineage from DataFrame operations."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    method = node.func.attr
                    if method == 'withColumn':
                        self._handle_with_column(node)
                    elif method == 'select':
                        self._handle_select(node)

    def _handle_with_column(self, node: ast.Call):
        """Handle withColumn operations."""
        if len(node.args) >= 2:
            col_name = self._get_const(node.args[0])
            deps = self._get_deps(node.args[1])
            if col_name:
                self.lineage[col_name] = sorted(deps)

    def _handle_select(self, node: ast.Call):
        """Handle select operations."""
        for arg in node.args:
            deps = self._get_deps(arg)
            if deps:
                col_name = self._get_const(arg)
                if col_name:
                    self.lineage[col_name] = sorted(deps)

    def _get_deps(self, node: ast.AST) -> Set[str]:
        """Get column dependencies from an expression."""
        deps = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Call):
                if isinstance(n.func, ast.Attribute) and n.func.attr == 'col':
                    if n.args:
                        dep = self._get_const(n.args[0])
                        if dep:
                            deps.add(dep)
                elif isinstance(n.func, ast.Name) and n.func.id in self.udfs:
                    deps.update(self.udfs[n.func.id])
        return deps

    def _get_const(self, node: ast.AST) -> str:
        """Extract string constant from AST node."""
        if isinstance(node, ast.Constant):
            return str(node.value)
        return ""
