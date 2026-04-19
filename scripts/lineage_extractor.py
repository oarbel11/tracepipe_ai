from typing import Dict, List, Optional
import re

class LineageExtractor:
    def __init__(self):
        self.lineage_map: Dict[str, List[str]] = {}

    def extract_from_sql(self, sql: str, target_table: str) -> List[str]:
        upstream = []
        from_pattern = r'FROM\s+([\w.]+)'
        join_pattern = r'JOIN\s+([\w.]+)'
        from_matches = re.findall(from_pattern, sql, re.IGNORECASE)
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
        upstream.extend(from_matches)
        upstream.extend(join_matches)
        self.lineage_map[target_table] = list(set(upstream))
        return upstream

    def extract_from_spark_plan(self, plan: str) -> Dict[str, List[str]]:
        lineage = {}
        relation_pattern = r'Relation\[.*?\]\s+([\w.]+)'
        relations = re.findall(relation_pattern, plan)
        if relations:
            lineage['sources'] = list(set(relations))
        return lineage

    def get_lineage(self, table: str) -> List[str]:
        return self.lineage_map.get(table, [])

    def get_all_lineage(self) -> Dict[str, List[str]]:
        return self.lineage_map

    def build_dependency_chain(self, table: str, visited: Optional[set] = None) -> List[str]:
        if visited is None:
            visited = set()
        if table in visited:
            return []
        visited.add(table)
        chain = [table]
        upstream = self.lineage_map.get(table, [])
        for upstream_table in upstream:
            chain.extend(self.build_dependency_chain(upstream_table, visited))
        return chain
