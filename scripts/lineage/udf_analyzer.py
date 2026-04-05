import re
import ast
from typing import Dict, List, Set

class UDFAnalyzer:
    def __init__(self):
        self.input_params = []
        self.output_dependencies = {}

    def analyze_udf(self, code: str) -> Dict:
        try:
            tree = ast.parse(code)
            return self._analyze_ast(tree)
        except SyntaxError:
            return self._fallback_regex_analysis(code)

    def _analyze_ast(self, tree: ast.AST) -> Dict:
        lineage = {"inputs": [], "outputs": [], "transformations": []}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                lineage["inputs"] = [arg.arg for arg in node.args.args]
                lineage["outputs"] = self._extract_return_deps(node)
                lineage["transformations"] = self._extract_transformations(node)
        return lineage

    def _extract_return_deps(self, func_node: ast.FunctionDef) -> List[str]:
        deps = []
        for node in ast.walk(func_node):
            if isinstance(node, ast.Return) and node.value:
                deps.extend(self._get_variable_names(node.value))
        return deps

    def _get_variable_names(self, node: ast.AST) -> List[str]:
        names = []
        for n in ast.walk(node):
            if isinstance(n, ast.Name):
                names.append(n.id)
            elif isinstance(n, ast.Attribute):
                names.append(n.attr)
        return names

    def _extract_transformations(self, func_node: ast.FunctionDef) -> List[str]:
        transforms = []
        for node in ast.walk(func_node):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    transforms.append(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    transforms.append(node.func.attr)
        return list(set(transforms))

    def _fallback_regex_analysis(self, code: str) -> Dict:
        lineage = {"inputs": [], "outputs": [], "transformations": [], "confidence": 0.5}
        func_match = re.search(r'def\s+\w+\(([^)]*)\)', code)
        if func_match:
            params = func_match.group(1).split(',')
            lineage["inputs"] = [p.strip().split(':')[0].strip() for p in params if p.strip()]
        return_matches = re.findall(r'return\s+([\w.]+)', code)
        lineage["outputs"] = return_matches
        lineage["transformations"] = re.findall(r'\.(\w+)\(', code)
        return lineage

    def infer_column_mapping(self, udf_name: str, input_columns: List[str], 
                            udf_metadata: Dict) -> Dict[str, List[str]]:
        mapping = {}
        inputs = udf_metadata.get("inputs", [])
        outputs = udf_metadata.get("outputs", [])
        for i, inp in enumerate(inputs):
            if i < len(input_columns):
                for out in outputs:
                    if out not in mapping:
                        mapping[out] = []
                    mapping[out].append(input_columns[i])
        return mapping
