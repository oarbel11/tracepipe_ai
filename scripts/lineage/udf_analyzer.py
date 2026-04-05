"""Analyze UDF code to infer column mappings."""
import ast
import re
from typing import List, Dict
from scripts.lineage.lineage_parser import ColumnLineage


class UDFAnalyzer:
    """Analyze UDF code to extract column dependencies."""

    def analyze_python_udf(self, udf_code: str) -> List[str]:
        """Analyze Python UDF to extract input column references."""
        try:
            tree = ast.parse(udf_code)
            params = []
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    params = [arg.arg for arg in node.args.args]
                    break
            return params
        except SyntaxError:
            return self._extract_params_regex(udf_code)

    def _extract_params_regex(self, code: str) -> List[str]:
        """Fallback regex extraction of function parameters."""
        match = re.search(r'def\s+\w+\s*\(([^)]*)\)', code)
        if match:
            params = match.group(1).split(',')
            return [p.strip().split(':')[0].strip() for p in params if p.strip()]
        return []

    def analyze_scala_udf(self, udf_code: str) -> List[str]:
        """Analyze Scala UDF to extract input parameters."""
        match = re.search(r'\(([^)]*)\)\s*=>', udf_code)
        if match:
            params = match.group(1).split(',')
            return [p.strip().split(':')[0].strip() for p in params if p.strip()]
        return []

    def create_udf_lineage(self, udf_name: str, udf_code: str,
                          target_column: str, language: str = 'python') -> ColumnLineage:
        """Create lineage info from UDF analysis."""
        if language.lower() == 'python':
            sources = self.analyze_python_udf(udf_code)
        elif language.lower() == 'scala':
            sources = self.analyze_scala_udf(udf_code)
        else:
            sources = []

        return ColumnLineage(
            target_column=target_column,
            source_columns=sources,
            transformation=f"UDF: {udf_name}"
        )
