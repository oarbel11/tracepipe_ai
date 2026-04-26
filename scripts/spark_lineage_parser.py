import re
import ast
from typing import Dict, List, Set, Tuple, Optional
from scripts.udf_lineage_tracker import UDFLineageTracker

class SparkLineageParser:
    def __init__(self):
        self.tables: Dict[str, Set[str]] = {}
        self.column_lineage: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
        self.udf_tracker = UDFLineageTracker()
    
    def parse_spark_code(self, code: str) -> Dict:
        self.udf_tracker.extract_udf_lineage(code=code)
        self._extract_table_reads(code)
        self._extract_column_transformations(code)
        return self._build_lineage_graph()
    
    def _extract_table_reads(self, code: str):
        read_patterns = [
            r'spark\.read\.table\(["\']([^"\')]+)["\']\)',
            r'spark\.table\(["\']([^"\')]+)["\']\)',
            r'FROM\s+([\w.]+)',
        ]
        for pattern in read_patterns:
            for match in re.finditer(pattern, code, re.IGNORECASE):
                table_name = match.group(1)
                self.tables[table_name] = set()
    
    def _extract_column_transformations(self, code: str):
        select_pattern = r'\.select\(([^)]+)\)'
        for match in re.finditer(select_pattern, code):
            cols = [c.strip().strip('"\' ') for c in match.group(1).split(',')]
            for col in cols:
                col_clean = re.sub(r'col\(["\']([^"\')]+)["\']\)', r'\1', col)
                if ' as ' in col_clean.lower():
                    parts = re.split(r'\s+as\s+', col_clean, flags=re.IGNORECASE)
                    if len(parts) == 2:
                        source, target = parts[0].strip(), parts[1].strip()
                        self.column_lineage[('output', target)] = {('input', source)}
        
        for col_name, deps in self.udf_tracker.column_lineage.items():
            self.column_lineage[('output', col_name)] = {('input', d) for d in deps}
    
    def _build_lineage_graph(self) -> Dict:
        return {
            "tables": list(self.tables.keys()),
            "column_lineage": {
                f"{t}.{c}": [f"{st}.{sc}" for st, sc in sources]
                for (t, c), sources in self.column_lineage.items()
            },
            "udfs": list(self.udf_tracker.udfs.keys())
        }
    
    def get_column_lineage(self, table: str, column: str) -> List[str]:
        key = (table, column)
        if key in self.column_lineage:
            return [f"{t}.{c}" for t, c in self.column_lineage[key]]
        return []
