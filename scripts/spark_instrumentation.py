import ast
import json
from typing import Dict, List, Any

class SparkInstrumentation:
    def __init__(self):
        self.lineage_data = []

    def extract_lineage_from_code(self, code: str) -> List[Dict[str, Any]]:
        """Extract column lineage from Spark code via AST analysis."""
        lineage = []
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    lineage.extend(self._analyze_call(node))
        except SyntaxError:
            pass
        return lineage

    def _analyze_call(self, node: ast.Call) -> List[Dict[str, Any]]:
        """Analyze function calls for lineage patterns."""
        lineage = []
        func_name = self._get_func_name(node)
        if func_name in ['select', 'withColumn', 'filter', 'groupBy']:
            lineage.append({
                'operation': func_name,
                'columns': self._extract_columns(node),
                'type': 'transformation'
            })
        return lineage

    def _get_func_name(self, node: ast.Call) -> str:
        """Get function name from call node."""
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        elif isinstance(node.func, ast.Name):
            return node.func.id
        return ''

    def _extract_columns(self, node: ast.Call) -> List[str]:
        """Extract column names from call arguments."""
        columns = []
        for arg in node.args:
            if isinstance(arg, ast.Constant):
                columns.append(str(arg.value))
            elif isinstance(arg, ast.Name):
                columns.append(arg.id)
        return columns

    def capture_runtime_lineage(self, execution_context: Dict) -> Dict[str, Any]:
        """Capture lineage from runtime execution context."""
        return {
            'notebook_path': execution_context.get('notebook_path', ''),
            'timestamp': execution_context.get('timestamp', ''),
            'transformations': execution_context.get('transformations', []),
            'source_tables': execution_context.get('source_tables', []),
            'target_tables': execution_context.get('target_tables', [])
        }

    def instrument_udf(self, udf_code: str) -> Dict[str, Any]:
        """Instrument UDF for lineage tracking."""
        lineage = self.extract_lineage_from_code(udf_code)
        return {
            'udf_lineage': lineage,
            'instrumented': True
        }
