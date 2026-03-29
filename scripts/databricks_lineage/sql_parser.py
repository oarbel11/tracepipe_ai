"""SQL parser for extracting table lineage."""

import re
from typing import List, Set, Dict, Any


class SQLLineageParser:
    """Parse SQL queries to extract table lineage."""

    def __init__(self):
        self.from_pattern = re.compile(
            r'\bFROM\s+([\w.`]+)',
            re.IGNORECASE
        )
        self.join_pattern = re.compile(
            r'\bJOIN\s+([\w.`]+)',
            re.IGNORECASE
        )
        self.insert_pattern = re.compile(
            r'\bINTO\s+(?:TABLE\s+)?([\w.`]+)',
            re.IGNORECASE
        )
        self.create_pattern = re.compile(
            r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+([\w.`]+)',
            re.IGNORECASE
        )

    def _clean_table_name(self, name: str) -> str:
        """Clean table name by removing quotes and extra spaces."""
        return name.strip().replace('`', '').replace('"', '')

    def extract_source_tables(self, sql: str) -> Set[str]:
        """Extract source tables from SQL query."""
        sources = set()
        sources.update(self.from_pattern.findall(sql))
        sources.update(self.join_pattern.findall(sql))
        return {self._clean_table_name(s) for s in sources}

    def extract_target_tables(self, sql: str) -> Set[str]:
        """Extract target tables from SQL query."""
        targets = set()
        targets.update(self.insert_pattern.findall(sql))
        targets.update(self.create_pattern.findall(sql))
        return {self._clean_table_name(t) for t in targets}

    def parse_lineage(self, sql: str) -> Dict[str, Any]:
        """Parse SQL and return lineage information."""
        sources = self.extract_source_tables(sql)
        targets = self.extract_target_tables(sql)
        
        edges = []
        for target in targets:
            for source in sources:
                edges.append({
                    'source': source,
                    'target': target,
                    'type': 'table_dependency'
                })
        
        return {
            'sources': list(sources),
            'targets': list(targets),
            'edges': edges
        }
