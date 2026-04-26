import ast
import re
from typing import Dict, List, Set, Any

class SparkLineageParser:
    def __init__(self):
        self.lineage = {}
        self.udfs = {}
        self.current_table = None

    def parse_code(self, code: str) -> Dict[str, Any]:
        """Parse Spark code and return lineage information"""
        tree = ast.parse(code)
        self.visit_tree(tree)
        return {
            'lineage': self.lineage,
            'udfs': self.udfs,
            'tables': list(set(t for t in [self.current_table] if t))
        }

    def parse(self, execution_plan: str) -> Dict[str, List[str]]:
        """Parse Spark execution plan"""
        return self.lineage

    def visit_tree(self, tree: ast.AST):
        """Visit AST nodes to extract lineage"""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self.handle_call(node)
            elif isinstance(node, ast.FunctionDef):
                self.handle_function_def(node)

    def handle_call(self, node: ast.Call):
        """Handle function calls including DataFrame operations"""
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in ['select', 'withColumn', 'selectExpr']:
                self.extract_columns(node)
            elif method in ['read', 'table']:
                self.extract_source_table(node)
            elif method in ['write', 'saveAsTable']:
                self.extract_target_table(node)

    def handle_function_def(self, node: ast.FunctionDef):
        """Extract UDF definitions and their dependencies"""
        if any(d.id == 'udf' for d in node.decorator_list if isinstance(d, ast.Name)):
            deps = self.extract_udf_dependencies(node)
            self.udfs[node.name] = {'dependencies': deps}

    def extract_columns(self, node: ast.Call):
        """Extract column references from DataFrame operations"""
        for arg in node.args:
            cols = self.get_column_names(arg)
            for col in cols:
                if self.current_table:
                    qualified = f"{self.current_table}.{col}"
                    if qualified not in self.lineage:
                        self.lineage[qualified] = []

    def extract_source_table(self, node: ast.Call):
        """Extract source table name"""
        if node.args and isinstance(node.args[0], ast.Constant):
            self.current_table = node.args[0].value.split('.')[-1]

    def extract_target_table(self, node: ast.Call):
        """Extract target table name"""
        for kw in node.keywords:
            if kw.arg == 'name' and isinstance(kw.value, ast.Constant):
                return kw.value.value

    def get_column_names(self, node: ast.AST) -> List[str]:
        """Extract column names from AST node"""
        cols = []
        if isinstance(node, ast.Constant):
            cols.append(node.value)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == 'col' and node.args:
                if isinstance(node.args[0], ast.Constant):
                    cols.append(node.args[0].value)
        return cols

    def extract_udf_dependencies(self, node: ast.FunctionDef) -> List[str]:
        """Extract column dependencies from UDF body"""
        deps = []
        for n in ast.walk(node):
            if isinstance(n, ast.Name):
                deps.append(n.id)
        return list(set(deps))
