"""Parse Spark DataFrame operations to extract column transformations."""

import ast
import re
from typing import Dict, List, Set, Tuple


class SparkColumnParser:
    """Extracts column-level transformations from Spark code."""

    def __init__(self):
        self.column_mappings: List[Dict] = []

    def parse_python_code(self, code: str) -> List[Dict]:
        """Parse Python Spark code and extract column lineage."""
        try:
            tree = ast.parse(code)
            self._visit_nodes(tree)
            return self.column_mappings
        except SyntaxError:
            return []

    def _visit_nodes(self, node: ast.AST) -> None:
        """Visit AST nodes to find DataFrame operations."""
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                self._analyze_call(child)

    def _analyze_call(self, node: ast.Call) -> None:
        """Analyze function calls for Spark operations."""
        func_name = self._get_func_name(node)
        
        if func_name in ["select", "withColumn", "selectExpr"]:
            self._extract_select_lineage(node, func_name)
        elif func_name in ["join", "unionByName"]:
            self._extract_join_lineage(node, func_name)

    def _get_func_name(self, node: ast.Call) -> str:
        """Extract function name from call node."""
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        elif isinstance(node.func, ast.Name):
            return node.func.id
        return ""

    def _extract_select_lineage(self, node: ast.Call, op: str) -> None:
        """Extract lineage from select/withColumn operations."""
        for arg in node.args:
            if isinstance(arg, ast.Constant):
                col_name = arg.value
                self.column_mappings.append({
                    "operation": op,
                    "target": col_name,
                    "sources": [col_name],
                    "expression": col_name
                })

    def _extract_join_lineage(self, node: ast.Call, op: str) -> None:
        """Extract lineage from join operations."""
        self.column_mappings.append({
            "operation": op,
            "target": "*",
            "sources": ["left.*", "right.*"],
            "expression": f"{op} operation"
        })

    def parse_scala_code(self, code: str) -> List[Dict]:
        """Parse Scala Spark code using regex patterns."""
        patterns = [
            (r'\.select\(([^)]+)\)', 'select'),
            (r'\.withColumn\("([^"]+)"', 'withColumn'),
        ]
        
        for pattern, op in patterns:
            for match in re.finditer(pattern, code):
                self.column_mappings.append({
                    "operation": op,
                    "target": match.group(1).strip('"'),
                    "sources": [match.group(1).strip('"')],
                    "expression": match.group(0)
                })
        
        return self.column_mappings
