"""UDF analyzer for extracting column dependencies."""

import ast
from typing import Dict, Optional, Set


class UDFAnalyzer:
    """Analyze UDFs to extract column-level dependencies."""

    def analyze_udf(self, func_node: ast.FunctionDef) -> Optional[Dict]:
        """Analyze a function definition to extract UDF information."""
        decorators = [d.id if isinstance(d, ast.Name) else None
                      for d in func_node.decorator_list]

        if "udf" not in decorators and "pandas_udf" not in decorators:
            return None

        params = [arg.arg for arg in func_node.args.args]
        accessed_cols = self._extract_column_accesses(func_node)

        return {
            "name": func_node.name,
            "type": "udf",
            "params": params,
            "accesses": list(accessed_cols),
        }

    def _extract_column_accesses(self, node: ast.AST) -> Set[str]:
        """Extract column names accessed in the function."""
        accesses = set()

        for child in ast.walk(node):
            if isinstance(child, ast.Subscript):
                if isinstance(child.value, ast.Name):
                    if isinstance(child.slice, ast.Constant):
                        accesses.add(child.slice.value)
            elif isinstance(child, ast.Attribute):
                if isinstance(child.value, ast.Name):
                    accesses.add(child.attr)

        return accesses
