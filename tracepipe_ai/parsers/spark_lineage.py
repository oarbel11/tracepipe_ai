"""Spark lineage parser with UDF and complex transformation support."""

import ast
import re
from typing import Dict, List, Set


class SparkLineageParser:
    """Parser for Spark DataFrame operations and UDFs."""

    def __init__(self):
        self.udfs: Dict[str, Dict] = {}
        self.tables: Dict[str, str] = {}
        self.column_lineage: Dict[str, Set[str]] = {}

    def parse_code(self, code: str) -> Dict:
        """Parse Spark code and return lineage information."""
        return self.parse(code)

    def parse(self, code: str) -> Dict:
        """Parse Spark code for lineage."""
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    if any(d.id == "udf" for d in node.decorator_list if isinstance(d, ast.Name)):
                        self._parse_udf(node)
                elif isinstance(node, ast.Assign):
                    self._parse_assignment(node)
        except Exception:
            pass
        return {"udfs": self.udfs, "tables": self.tables, "column_lineage": self._get_qualified_lineage()}

    def _parse_udf(self, node: ast.FunctionDef):
        """Extract UDF information."""
        inputs = [arg.arg for arg in node.args.args]
        outputs = []
        for n in ast.walk(node):
            if isinstance(n, ast.Return) and n.value:
                outputs.append("result")
        self.udfs[node.name] = {"inputs": inputs, "outputs": outputs, "transforms": ["custom"]}

    def _parse_assignment(self, node: ast.Assign):
        """Parse DataFrame assignments."""
        if not node.targets or not isinstance(node.targets[0], ast.Name):
            return
        var_name = node.targets[0].id
        if isinstance(node.value, ast.Call):
            self._track_dataframe_call(var_name, node.value)

    def _track_dataframe_call(self, var_name: str, call: ast.Call):
        """Track DataFrame operations."""
        if isinstance(call.func, ast.Attribute):
            if call.func.attr in ["select", "withColumn", "join"]:
                self._track_column_lineage(var_name, call)

    def _track_column_lineage(self, target: str, call: ast.Call):
        """Track column-level lineage with qualified names."""
        if target not in self.column_lineage:
            self.column_lineage[target] = set()
        for arg in call.args:
            cols = self._extract_columns(arg)
            self.column_lineage[target].update(cols)

    def _extract_columns(self, node) -> Set[str]:
        """Extract column references from AST node."""
        cols = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Str):
                cols.add(n.s)
            elif isinstance(n, ast.Constant) and isinstance(n.value, str):
                cols.add(n.value)
        return cols

    def _get_qualified_lineage(self) -> Dict[str, List[str]]:
        """Return column lineage with qualified names."""
        result = {}
        for target, cols in self.column_lineage.items():
            qualified = [f"{target}.{col}" if "." not in col else col for col in cols]
            result[target] = sorted(qualified)
        return result
