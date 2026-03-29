"""Databricks-specific lineage extractor for UDFs, DML, and file operations."""
import re
from typing import Dict, List, Set, Any


class DatabricksLineageExtractor:
    """Extract lineage from Databricks-specific operations."""

    def __init__(self):
        self.udf_pattern = re.compile(r'\b(\w+)\s*\(', re.IGNORECASE)
        self.dml_pattern = re.compile(
            r'\b(UPDATE|DELETE|INSERT|MERGE)\b', re.IGNORECASE
        )
        self.file_pattern = re.compile(
            r'\b(LOCATION|PATH)\s+[\'"]([^\'"]+)[\'"]', re.IGNORECASE
        )

    def extract_lineage(self, query: str, plan: str = "") -> Dict[str, Any]:
        """Extract lineage information from query and execution plan."""
        lineage = {
            "udfs": self._extract_udfs(query),
            "dml_operations": self._extract_dml(query),
            "file_operations": self._extract_file_ops(query),
            "columns": self._extract_columns(query),
            "tables": self._extract_tables(query)
        }
        return lineage

    def _extract_udfs(self, query: str) -> List[str]:
        """Extract UDF names from query."""
        udfs = []
        keywords = {'SELECT', 'FROM', 'WHERE', 'JOIN', 'AND', 'OR', 'ON'}
        matches = self.udf_pattern.findall(query)
        for match in matches:
            if match.upper() not in keywords:
                udfs.append(match)
        return list(set(udfs))

    def _extract_dml(self, query: str) -> List[str]:
        """Extract DML operation types."""
        matches = self.dml_pattern.findall(query)
        return [m.upper() for m in matches]

    def _extract_file_ops(self, query: str) -> List[str]:
        """Extract file paths from query."""
        matches = self.file_pattern.findall(query)
        return [path for _, path in matches]

    def _extract_columns(self, query: str) -> List[str]:
        """Extract column names from SELECT clause."""
        select_match = re.search(
            r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return []
        cols_str = select_match.group(1)
        if '*' in cols_str:
            return ['*']
        cols = [c.strip().split()[-1] for c in cols_str.split(',')]
        return [c for c in cols if c]

    def _extract_tables(self, query: str) -> List[str]:
        """Extract table names from query."""
        from_match = re.findall(
            r'FROM\s+([\w.]+)', query, re.IGNORECASE
        )
        join_match = re.findall(
            r'JOIN\s+([\w.]+)', query, re.IGNORECASE
        )
        return list(set(from_match + join_match))
