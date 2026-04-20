"""DataFrame operation tracker for lineage."""

import ast
from typing import List, Dict, Optional


class DataFrameTracker:
    """Track DataFrame operations to build column lineage."""

    def track_operation(self, call_node: ast.Call) -> List[Dict]:
        """Track a DataFrame operation and extract lineage."""
        if not isinstance(call_node.func, ast.Attribute):
            return []

        method = call_node.func.attr

        if method == "withColumn":
            return self._track_with_column(call_node)
        elif method == "select":
            return self._track_select(call_node)
        elif method == "join":
            return self._track_join(call_node)

        return []

    def _track_with_column(self, node: ast.Call) -> List[Dict]:
        """Track withColumn operation."""
        if len(node.args) < 2:
            return []

        target = self._extract_string(node.args[0])
        sources = self._extract_columns(node.args[1])

        if target:
            return [{"target": target, "sources": sources, "op": "withColumn"}]
        return []

    def _track_select(self, node: ast.Call) -> List[Dict]:
        """Track select operation."""
        lineage = []
        for arg in node.args:
            cols = self._extract_columns(arg)
            for col in cols:
                lineage.append({"target": col, "sources": [col], "op": "select"})
        return lineage

    def _track_join(self, node: ast.Call) -> List[Dict]:
        """Track join operation."""
        return [{"target": "*", "sources": ["*"], "op": "join"}]

    def _extract_string(self, node: ast.AST) -> Optional[str]:
        """Extract string value from AST node."""
        if isinstance(node, ast.Constant):
            return str(node.value)
        return None

    def _extract_columns(self, node: ast.AST) -> List[str]:
        """Extract column references from expression."""
        cols = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Name) and child.func.id == "col":
                    if child.args and isinstance(child.args[0], ast.Constant):
                        cols.append(child.args[0].value)
        return cols
