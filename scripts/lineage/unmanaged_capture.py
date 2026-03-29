import re
import sqlparse
from typing import List, Dict, Set
from pathlib import Path


class UnmanagedLineageCapture:
    def __init__(self):
        self.lineage_records = []
        self.file_write_patterns = [
            r'\.write\s*\.(?:parquet|csv|json|orc|avro)\s*\(',
            r'\.save\s*\([\'"](?!delta|jdbc)[^'"]+['"]\)',
            r'to_(?:parquet|csv|json)\s*\(',
        ]
    
    def extract_from_sql(self, sql_content: str) -> List[Dict]:
        lineage = []
        parsed = sqlparse.parse(sql_content)
        
        for stmt in parsed:
            if self._is_ctas_unmanaged(stmt):
                lineage.append(self._extract_ctas_lineage(stmt))
            elif self._has_location_clause(stmt):
                lineage.append(self._extract_location_lineage(stmt))
        
        return lineage
    
    def extract_from_python(self, python_code: str) -> List[Dict]:
        lineage = []
        lines = python_code.split('\n')
        
        for i, line in enumerate(lines):
            for pattern in self.file_write_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    lineage.append(self._extract_python_write(line, i))
        
        return lineage
    
    def _is_ctas_unmanaged(self, stmt) -> bool:
        stmt_str = str(stmt).upper()
        return 'CREATE TABLE' in stmt_str and 'LOCATION' in stmt_str
    
    def _has_location_clause(self, stmt) -> bool:
        return 'LOCATION' in str(stmt).upper()
    
    def _extract_ctas_lineage(self, stmt) -> Dict:
        stmt_str = str(stmt)
        table_match = re.search(r'CREATE\s+TABLE\s+(\S+)', stmt_str, re.I)
        location_match = re.search(r"LOCATION\s+['\"]([^'\"]+)", stmt_str, re.I)
        source_match = re.search(r'FROM\s+(\S+)', stmt_str, re.I)
        
        return {
            'type': 'unmanaged_table',
            'target': table_match.group(1) if table_match else 'unknown',
            'location': location_match.group(1) if location_match else None,
            'source': source_match.group(1) if source_match else 'unknown',
            'columns': self._extract_columns(stmt_str)
        }
    
    def _extract_location_lineage(self, stmt) -> Dict:
        stmt_str = str(stmt)
        location_match = re.search(r"LOCATION\s+['\"]([^'\"]+)", stmt_str, re.I)
        
        return {
            'type': 'direct_write',
            'location': location_match.group(1) if location_match else 'unknown',
            'statement': stmt_str[:200]
        }
    
    def _extract_python_write(self, line: str, line_num: int) -> Dict:
        path_match = re.search(r"['\"]([^'\"]+\.(?:parquet|csv|json|orc|avro))['\"]?", line)
        
        return {
            'type': 'python_write',
            'location': path_match.group(1) if path_match else 'unknown',
            'line_number': line_num + 1,
            'code': line.strip()
        }
    
    def _extract_columns(self, stmt_str: str) -> List[str]:
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', stmt_str, re.I | re.DOTALL)
        if not select_match:
            return []
        
        cols_str = select_match.group(1)
        return [c.strip().split()[-1] for c in cols_str.split(',')]
