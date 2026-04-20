import ast
import re
from typing import Dict, List, Set, Any

class SparkLineageParser:
    def __init__(self):
        self.udf_analyzer = UDFAnalyzer()
        self.df_tracker = DataFrameOperationTracker()

    def parse_code(self, code: str) -> Dict[str, Any]:
        """Parse Spark code and extract column lineage."""
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return {"tables": [], "columns": {}, "operations": []}
        
        lineage = {"tables": [], "columns": {}, "operations": []}
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self._process_call(node, lineage, code)
        
        return lineage

    def _process_call(self, node: ast.Call, lineage: Dict, code: str):
        """Process function calls to extract lineage."""
        func_name = self._get_func_name(node)
        
        if func_name in ['select', 'selectExpr']:
            self._handle_select(node, lineage)
        elif func_name == 'withColumn':
            self._handle_with_column(node, lineage)
        elif func_name in ['udf', 'pandas_udf']:
            self._handle_udf(node, lineage, code)
        elif func_name in ['groupBy', 'agg', 'filter', 'where']:
            self._handle_transformation(node, lineage, func_name)

    def _get_func_name(self, node: ast.Call) -> str:
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        elif isinstance(node.func, ast.Name):
            return node.func.id
        return ""

    def _handle_select(self, node: ast.Call, lineage: Dict):
        for arg in node.args:
            col_name = self._extract_column_name(arg)
            if col_name:
                lineage["columns"][col_name] = [col_name]
        lineage["operations"].append({"type": "select", "columns": list(lineage["columns"].keys())})

    def _handle_with_column(self, node: ast.Call, lineage: Dict):
        if len(node.args) >= 2:
            new_col = self._extract_column_name(node.args[0])
            deps = self._extract_dependencies(node.args[1])
            if new_col:
                lineage["columns"][new_col] = deps
                lineage["operations"].append({"type": "withColumn", "column": new_col, "dependencies": deps})

    def _handle_udf(self, node: ast.Call, lineage: Dict, code: str):
        if node.args:
            udf_info = self.udf_analyzer.analyze_udf(node.args[0], code)
            lineage["operations"].append({"type": "udf", "info": udf_info})

    def _handle_transformation(self, node: ast.Call, lineage: Dict, op_type: str):
        cols = [self._extract_column_name(arg) for arg in node.args]
        lineage["operations"].append({"type": op_type, "columns": [c for c in cols if c]})

    def _extract_column_name(self, node) -> str:
        if isinstance(node, ast.Constant):
            return str(node.value)
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Call) and self._get_func_name(node) == 'col':
            if node.args:
                return self._extract_column_name(node.args[0])
        return ""

    def _extract_dependencies(self, node) -> List[str]:
        deps = []
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and self._get_func_name(n) == 'col':
                col_name = self._extract_column_name(n.args[0]) if n.args else ""
                if col_name:
                    deps.append(col_name)
        return deps

class UDFAnalyzer:
    def analyze_udf(self, func_node, code: str) -> Dict[str, Any]:
        return {"detected": True, "input_columns": [], "output_columns": []}

class DataFrameOperationTracker:
    pass
