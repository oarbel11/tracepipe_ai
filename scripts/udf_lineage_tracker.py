import ast
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field

@dataclass
class UDFMetadata:
    name: str
    input_columns: List[str] = field(default_factory=list)
    output_column: str = ""
    code: str = ""
    dependencies: Set[str] = field(default_factory=set)

class UDFASTVisitor(ast.NodeVisitor):
    def __init__(self):
        self.column_refs = set()
        self.function_calls = set()
    
    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name):
            self.column_refs.add(f"{node.value.id}.{node.attr}")
        self.generic_visit(node)
    
    def visit_Subscript(self, node):
        if isinstance(node.value, ast.Name) and isinstance(node.slice, ast.Constant):
            self.column_refs.add(f"{node.value.id}['{node.slice.value}']")
        self.generic_visit(node)

class UDFLineageTracker:
    def __init__(self):
        self.udfs: Dict[str, UDFMetadata] = {}
        self.column_lineage: Dict[str, Set[str]] = {}
    
    def parse_udf_definition(self, code: str, udf_name: str) -> UDFMetadata:
        try:
            tree = ast.parse(code)
            visitor = UDFASTVisitor()
            visitor.visit(tree)
            return UDFMetadata(
                name=udf_name,
                code=code,
                dependencies=visitor.column_refs
            )
        except:
            return UDFMetadata(name=udf_name, code=code)
    
    def extract_udf_from_code(self, code: str) -> List[UDFMetadata]:
        udf_pattern = r'@udf\s*\(.*?\)\s*def\s+(\w+)\s*\((.*?)\):[\s\S]*?(?=\n@|\ndef\s|\nclass\s|$)'
        matches = re.finditer(udf_pattern, code, re.MULTILINE)
        udfs = []
        for match in matches:
            udf_name = match.group(1)
            udf_code = match.group(0)
            udfs.append(self.parse_udf_definition(udf_code, udf_name))
        return udfs
    
    def track_withColumn(self, df_name: str, col_name: str, expression: str) -> Set[str]:
        deps = set()
        col_refs = re.findall(r'col\(["\']([^"\')]+)["\']\)', expression)
        deps.update(col_refs)
        for udf_name, udf_meta in self.udfs.items():
            if udf_name in expression:
                deps.update(udf_meta.dependencies)
        return deps
    
    def extract_udf_lineage(self, notebook_path: str = None, code: str = None) -> 'UDFLineageTracker':
        if notebook_path:
            with open(notebook_path, 'r') as f:
                code = f.read()
        
        for udf in self.extract_udf_from_code(code):
            self.udfs[udf.name] = udf
        
        withcol_pattern = r'\.withColumn\(["\']([^"\')]+)["\'],\s*(.+?)\)'
        for match in re.finditer(withcol_pattern, code):
            col_name = match.group(1)
            expr = match.group(2)
            self.column_lineage[col_name] = self.track_withColumn('df', col_name, expr)
        
        return self
    
    def get_column_dependencies(self, table: str, column: str) -> Dict[str, List[str]]:
        if column in self.column_lineage:
            return {"source_columns": list(self.column_lineage[column])}
        return {"source_columns": []}
