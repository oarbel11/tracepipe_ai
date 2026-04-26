import ast
from typing import Dict, List, Set

class UDFAnalyzer:
    def __init__(self):
        self.udf_definitions = {}
        self.column_dependencies = {}

    def analyze_udf(self, udf_code: str, udf_name: str) -> Dict[str, List[str]]:
        """Analyze UDF code to extract column dependencies"""
        try:
            tree = ast.parse(udf_code)
            deps = self._extract_dependencies(tree)
            self.udf_definitions[udf_name] = deps
            return {udf_name: deps}
        except SyntaxError:
            return {udf_name: []}

    def _extract_dependencies(self, tree: ast.AST) -> List[str]:
        """Extract column references from UDF AST"""
        deps = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                if node.id not in ['return', 'def', 'self']:
                    deps.add(node.id)
            elif isinstance(node, ast.Attribute):
                if isinstance(node.value, ast.Name):
                    deps.add(node.attr)
        return list(deps)

    def track_udf_usage(self, df_operation: str, udf_name: str) -> Dict[str, List[str]]:
        """Track where UDFs are used in DataFrame operations"""
        if udf_name in self.udf_definitions:
            deps = self.udf_definitions[udf_name]
            self.column_dependencies[df_operation] = deps
            return {df_operation: deps}
        return {}

    def get_transitive_dependencies(self, column: str) -> Set[str]:
        """Get all transitive dependencies for a column"""
        deps = set()
        to_process = [column]
        while to_process:
            current = to_process.pop()
            if current in self.column_dependencies:
                for dep in self.column_dependencies[current]:
                    if dep not in deps:
                        deps.add(dep)
                        to_process.append(dep)
        return deps
