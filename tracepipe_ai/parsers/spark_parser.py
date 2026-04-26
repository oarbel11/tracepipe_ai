"""Spark lineage parser with UDF and complex transformation support."""
import ast
import inspect
from typing import Dict, List, Set, Any, Optional, Callable


class SparkLineageParser:
    """Enhanced Spark lineage parser supporting UDFs and transformations."""

    def __init__(self):
        self.udf_registry: Dict[str, Callable] = {}
        self.udf_lineage: Dict[str, List[str]] = {}

    def register_udf(self, name: str, func: Callable,
                     input_cols: Optional[List[str]] = None):
        """Register a UDF with its column dependencies."""
        self.udf_registry[name] = func
        if input_cols:
            self.udf_lineage[name] = input_cols
        else:
            self.udf_lineage[name] = self._extract_udf_dependencies(func)

    def _extract_udf_dependencies(self, func: Callable) -> List[str]:
        """Extract column dependencies from UDF source code."""
        try:
            source = inspect.getsource(func)
            tree = ast.parse(source)
            deps = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Subscript):
                    if isinstance(node.slice, ast.Constant):
                        deps.add(node.slice.value)
            return list(deps)
        except Exception:
            return []

    def parse_transformation(self, operation: str,
                           columns: List[str]) -> Dict[str, List[str]]:
        """Parse transformation to extract column lineage."""
        lineage = {}
        if "withColumn" in operation:
            parts = operation.split(",")
            if len(parts) >= 2:
                new_col = parts[0].split("(")[-1].strip().strip("'\"")
                deps = [c.strip() for c in columns if c in operation]
                lineage[new_col] = deps
        elif "select" in operation:
            for col in columns:
                lineage[col] = [col]
        return lineage

    def track_lineage(self, operations: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
        """Track lineage through multiple operations."""
        lineage: Dict[str, Set[str]] = {}
        for op in operations:
            op_type = op.get("type", "")
            if op_type == "udf":
                udf_name = op.get("name", "")
                output_col = op.get("output_col", "")
                if udf_name in self.udf_lineage:
                    lineage[output_col] = set(self.udf_lineage[udf_name])
            elif op_type == "transform":
                trans_lineage = self.parse_transformation(
                    op.get("operation", ""), op.get("columns", []))
                for col, deps in trans_lineage.items():
                    lineage[col] = set(deps)
        return lineage
