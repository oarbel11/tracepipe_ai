"""Map column lineage through UDFs."""

import ast
import inspect
from typing import Dict, List, Any, Optional, Callable


class UDFColumnMapper:
    """Maps column-level lineage through UDFs."""

    def __init__(self):
        self.udf_mappings: Dict[str, Dict[str, Any]] = {}

    def analyze_udf(self, udf_func: Callable, udf_name: str
                    ) -> Dict[str, Any]:
        """Analyze UDF to extract column mappings."""
        try:
            source = inspect.getsource(udf_func)
            tree = ast.parse(source)
            
            inputs = []
            outputs = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.arg):
                    inputs.append(node.arg)
                elif isinstance(node, ast.Return) and node.value:
                    outputs.append(ast.unparse(node.value))
            
            mapping = {
                'udf_name': udf_name,
                'inputs': inputs,
                'outputs': outputs,
                'source': source
            }
            self.udf_mappings[udf_name] = mapping
            return mapping
        except Exception:
            return {'udf_name': udf_name, 'inputs': [], 'outputs': []}

    def get_column_lineage(self, udf_name: str, input_cols: List[str]
                           ) -> List[str]:
        """Get output columns for given input columns."""
        if udf_name not in self.udf_mappings:
            return input_cols
        
        mapping = self.udf_mappings[udf_name]
        return mapping.get('outputs', input_cols)

    def register_manual_mapping(self, udf_name: str,
                                input_cols: List[str],
                                output_cols: List[str]):
        """Manually register column mapping."""
        self.udf_mappings[udf_name] = {
            'udf_name': udf_name,
            'inputs': input_cols,
            'outputs': output_cols,
            'manual': True
        }
