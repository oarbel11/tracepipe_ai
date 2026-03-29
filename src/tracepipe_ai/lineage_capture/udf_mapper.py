"""Map column lineage through UDFs."""

import ast
import inspect
from typing import Dict, List, Any, Callable, Optional


class UDFMapper:
    """Maps column transformations through user-defined functions."""

    def __init__(self):
        self.udf_registry: Dict[str, Dict[str, Any]] = {}

    def register_udf(
        self,
        name: str,
        func: Callable,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Register a UDF for lineage tracking."""
        udf_info = {
            'name': name,
            'function': func,
            'metadata': metadata or {},
            'signature': str(inspect.signature(func))
        }
        self.udf_registry[name] = udf_info
        return udf_info

    def analyze_udf(self, name: str) -> Dict[str, Any]:
        """Analyze a UDF to extract column dependencies."""
        if name not in self.udf_registry:
            return {'inputs': [], 'outputs': [], 'transformations': []}

        udf_info = self.udf_registry[name]
        func = udf_info['function']

        try:
            source = inspect.getsource(func)
            tree = ast.parse(source)
            inputs = []
            outputs = []

            for node in ast.walk(tree):
                if isinstance(node, ast.arg):
                    inputs.append(node.arg)
                elif isinstance(node, ast.Return) and isinstance(node.value, ast.Name):
                    outputs.append(node.value.id)

            return {
                'inputs': inputs,
                'outputs': outputs,
                'transformations': [{'type': 'udf', 'name': name}]
            }
        except Exception:
            return {'inputs': [], 'outputs': [], 'transformations': []}
