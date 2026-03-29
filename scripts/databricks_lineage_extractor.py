import re
import sqlparse
from typing import Dict, List, Set, Tuple

class DatabricksLineageExtractor:
    def __init__(self):
        self.udf_pattern = re.compile(r'\b(\w+)\s*\(', re.IGNORECASE)
        self.dml_pattern = re.compile(r'\b(UPDATE|DELETE|INSERT|MERGE)\b', re.IGNORECASE)
        self.file_write_pattern = re.compile(r'\b(COPY INTO|SAVE|WRITE)\b', re.IGNORECASE)

    def extract_operations(self, sql: str) -> Dict[str, List[str]]:
        operations = {'udf': [], 'dml': [], 'file_write': [], 'merge': []}
        
        dml_matches = self.dml_pattern.findall(sql)
        for op in dml_matches:
            if op.upper() == 'MERGE':
                operations['merge'].append(self._extract_merge_details(sql))
            else:
                operations['dml'].append(op.upper())
        
        if self.file_write_pattern.search(sql):
            operations['file_write'] = self._extract_file_paths(sql)
        
        return operations

    def _extract_merge_details(self, sql: str) -> str:
        merge_match = re.search(r'MERGE INTO\s+(\S+)', sql, re.IGNORECASE)
        return merge_match.group(1) if merge_match else 'unknown'

    def _extract_file_paths(self, sql: str) -> List[str]:
        path_pattern = re.compile(r"['\"]([^'\"]*(?:s3|dbfs|abfss)[^'\"]*)['\"]")
        return path_pattern.findall(sql)

    def extract_column_lineage(self, sql: str) -> Dict[str, Set[str]]:
        parsed = sqlparse.parse(sql)
        lineage = {}
        
        for statement in parsed:
            lineage.update(self._parse_statement(statement))
        
        return lineage

    def _parse_statement(self, statement) -> Dict[str, Set[str]]:
        lineage = {}
        tokens = [t for t in statement.flatten() if not t.is_whitespace]
        
        current_col = None
        for i, token in enumerate(tokens):
            if token.ttype is sqlparse.tokens.Name:
                if i > 0 and tokens[i-1].value.upper() in ('SELECT', ','):
                    current_col = token.value
                    lineage[current_col] = set()
                elif current_col:
                    lineage[current_col].add(token.value)
        
        return lineage

    def detect_python_udfs(self, query_plan: str) -> List[Dict[str, str]]:
        udf_matches = re.findall(r'PythonUDF.*?\[(.*?)\]', query_plan, re.DOTALL)
        udfs = []
        for match in udf_matches:
            udfs.append({'type': 'PythonUDF', 'details': match.strip()})
        return udfs
