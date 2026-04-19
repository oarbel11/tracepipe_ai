import json
import re
from typing import List, Dict, Any, Optional


class ColumnNode:
    """Represents a column-level lineage node"""
    def __init__(self, table: str, column: str, transformation: Optional[str] = None):
        self.table = table
        self.column = column
        self.transformation = transformation
        self.dependencies = []

    def add_dependency(self, node: 'ColumnNode'):
        self.dependencies.append(node)

    def __repr__(self):
        return f"{self.table}.{self.column}"


class LineageExtractor:
    """Extracts lineage from Databricks Unity Catalog and Spark queries"""

    def __init__(self):
        self.lineage_data = []

    def extract_from_query(self, query: str) -> Dict[str, Any]:
        """Extract lineage from SQL query"""
        query = query.lower().strip()
        lineage = {"sources": [], "target": None, "type": "query"}

        from_match = re.search(r'from\s+([\w.]+)', query)
        if from_match:
            lineage["sources"].append(from_match.group(1))

        join_matches = re.findall(r'join\s+([\w.]+)', query)
        lineage["sources"].extend(join_matches)

        create_match = re.search(r'create\s+(?:table|view)\s+([\w.]+)', query)
        insert_match = re.search(r'insert\s+into\s+([\w.]+)', query)

        if create_match:
            lineage["target"] = create_match.group(1)
        elif insert_match:
            lineage["target"] = insert_match.group(1)

        self.lineage_data.append(lineage)
        return lineage

    def extract_column_lineage(self, query: str) -> List[ColumnNode]:
        """Extract column-level lineage from query"""
        nodes = []
        select_match = re.search(r'select\s+(.+?)\s+from', query.lower())
        if select_match:
            columns = select_match.group(1).split(',')
            for col in columns:
                col = col.strip()
                if ' as ' in col:
                    expr, alias = col.split(' as ')
                    node = ColumnNode("target", alias.strip(), expr.strip())
                else:
                    node = ColumnNode("target", col)
                nodes.append(node)
        return nodes

    def get_lineage_graph(self) -> Dict[str, Any]:
        """Return complete lineage graph"""
        return {"nodes": self.lineage_data, "type": "spark_lineage"}
