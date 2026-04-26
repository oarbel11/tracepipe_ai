"""Spark SQL lineage parser with UDF support."""

import re
from typing import Dict, List, Set


class SparkLineageParser:
    """Parse Spark SQL and track column lineage including UDFs."""

    def __init__(self):
        self.udfs: Dict[str, Dict] = {}
        self.lineage: Dict[str, Set[str]] = {}

    def register_udf(self, name: str, input_cols: List[str], 
                     output_col: str) -> None:
        """Register a UDF with its column dependencies."""
        self.udfs[name] = {
            "input_cols": input_cols,
            "output_col": output_col
        }

    def parse_query(self, query: str) -> Dict[str, Set[str]]:
        """Parse SQL query and return column lineage."""
        self.lineage = {}
        query = query.strip()
        
        # Handle SELECT statements
        if query.upper().startswith("SELECT"):
            self._parse_select(query)
        
        return self.lineage

    def _parse_select(self, query: str) -> None:
        """Parse SELECT statement."""
        # Extract SELECT clause
        select_match = re.search(r"SELECT\s+(.+?)\s+FROM", query, 
                                 re.IGNORECASE | re.DOTALL)
        if not select_match:
            return
        
        select_clause = select_match.group(1)
        items = [s.strip() for s in select_clause.split(",")]
        
        for item in items:
            # Handle "col AS alias" or "col alias"
            alias_match = re.search(r"(.+?)\s+(?:AS\s+)?(\w+)$", item, 
                                    re.IGNORECASE)
            if alias_match and alias_match.group(2).upper() not in \
               ["FROM", "WHERE", "GROUP", "ORDER"]:
                expr = alias_match.group(1).strip()
                alias = alias_match.group(2)
            else:
                expr = item
                alias = item.split(".")[-1] if "." in item else item
            
            # Track dependencies
            deps = self._extract_dependencies(expr)
            if deps:
                self.lineage[alias] = deps

    def _extract_dependencies(self, expr: str) -> Set[str]:
        """Extract column dependencies from expression."""
        deps = set()
        
        # Check for UDF calls
        for udf_name, udf_info in self.udfs.items():
            if udf_name in expr:
                deps.update(udf_info["input_cols"])
        
        # Extract simple column references (word characters)
        cols = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", expr)
        for col in cols:
            if col.upper() not in ["SELECT", "FROM", "WHERE", "AS", 
                                    "AND", "OR", "NULL"]:
                deps.add(col)
        
        return deps
