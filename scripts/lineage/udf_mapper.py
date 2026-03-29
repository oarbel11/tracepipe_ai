import ast
import re
from typing import List, Dict, Set


class UDFColumnMapper:
    def __init__(self):
        self.udf_definitions = {}
        self.column_mappings = []
    
    def extract_udf_lineage(self, code: str, sql: str = None) -> List[Dict]:
        lineage = []
        
        python_udfs = self._parse_python_udfs(code)
        lineage.extend(python_udfs)
        
        if sql:
            sql_udfs = self._parse_sql_udfs(sql)
            lineage.extend(sql_udfs)
        
        return lineage
    
    def _parse_python_udfs(self, code: str) -> List[Dict]:
        udfs = []
        
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    if self._is_udf_decorator(node):
                        udfs.append(self._analyze_udf_function(node))
        except SyntaxError:
            pass
        
        return udfs
    
    def _is_udf_decorator(self, func_node) -> bool:
        for decorator in func_node.decorator_list:
            if isinstance(decorator, ast.Name) and 'udf' in decorator.id.lower():
                return True
            if isinstance(decorator, ast.Call):
                if hasattr(decorator.func, 'attr') and 'udf' in decorator.func.attr.lower():
                    return True
        return False
    
    def _analyze_udf_function(self, func_node) -> Dict:
        input_cols = [arg.arg for arg in func_node.args.args]
        output_deps = self._trace_dependencies(func_node)
        
        return {
            'type': 'udf',
            'name': func_node.name,
            'input_columns': input_cols,
            'output_dependencies': output_deps,
            'line_number': func_node.lineno
        }
    
    def _trace_dependencies(self, func_node) -> Dict[str, List[str]]:
        deps = {}
        
        for node in ast.walk(func_node):
            if isinstance(node, ast.Return) and node.value:
                deps['return'] = self._extract_var_names(node.value)
        
        return deps
    
    def _extract_var_names(self, node) -> List[str]:
        names = []
        for child in ast.walk(node):
            if isinstance(child, ast.Name):
                names.append(child.id)
        return names
    
    def _parse_sql_udfs(self, sql: str) -> List[Dict]:
        udfs = []
        
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)\s*\(([^)]*)\)\s+RETURNS\s+(\w+)\s+(?:RETURN|AS)\s+(.+?)(?:;|$)'
        matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            name, params, return_type, body = match.groups()
            udfs.append({
                'type': 'sql_udf',
                'name': name.strip(),
                'parameters': [p.strip().split()[0] for p in params.split(',') if p.strip()],
                'return_type': return_type.strip(),
                'body': body.strip()[:200],
                'column_mapping': self._infer_sql_udf_mapping(body)
            })
        
        return udfs
    
    def _infer_sql_udf_mapping(self, body: str) -> List[str]:
        referenced_cols = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', body)
        return list(set(referenced_cols))[:10]
