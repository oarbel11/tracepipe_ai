"""Enhanced lineage builder for Databricks operations."""
from typing import Dict, List, Any
from tracepipe_ai.databricks_lineage_extractor import DatabricksLineageExtractor


class LineageEnhancer:
    """Build enhanced lineage graphs for Databricks operations."""

    def __init__(self):
        self.extractor = DatabricksLineageExtractor()
        self.lineage_graph = {"nodes": [], "edges": []}

    def build_lineage(self, query: str, plan: str = "") -> Dict[str, Any]:
        """Build lineage graph from query and execution plan."""
        lineage_data = self.extractor.extract_lineage(query, plan)
        graph = self._create_graph(lineage_data)
        return {
            "lineage_data": lineage_data,
            "graph": graph
        }

    def _create_graph(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create graph representation from lineage data."""
        nodes = []
        edges = []
        node_id = 0

        table_ids = {}
        for table in lineage_data.get("tables", []):
            table_ids[table] = node_id
            nodes.append({"id": node_id, "type": "table", "name": table})
            node_id += 1

        for col in lineage_data.get("columns", []):
            col_id = node_id
            nodes.append({"id": col_id, "type": "column", "name": col})
            node_id += 1

        for udf in lineage_data.get("udfs", []):
            udf_id = node_id
            nodes.append({"id": udf_id, "type": "udf", "name": udf})
            node_id += 1

        for file_path in lineage_data.get("file_operations", []):
            file_id = node_id
            nodes.append({"id": file_id, "type": "file", "name": file_path})
            node_id += 1

        for i in range(len(nodes) - 1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"]})

        return {"nodes": nodes, "edges": edges}

    def get_column_lineage(self, column: str) -> List[Dict[str, Any]]:
        """Get lineage for a specific column."""
        result = []
        for node in self.lineage_graph.get("nodes", []):
            if node.get("type") == "column" and node.get("name") == column:
                result.append(node)
        return result
