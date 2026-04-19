from databricks.sdk import WorkspaceClient
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class ColumnNode:
    name: str
    table: str
    data_type: str
    upstream_columns: List[str] = None

    def __post_init__(self):
        if self.upstream_columns is None:
            self.upstream_columns = []

class LineageExtractor:
    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client
    
    def extract_table_lineage(self, table_name: str) -> Dict:
        try:
            lineage = self.client.lineage.get_table_lineage(
                table_name=table_name
            )
            return {
                "table": table_name,
                "upstream": [u.name for u in lineage.upstreams] if lineage.upstreams else [],
                "downstream": [d.name for d in lineage.downstreams] if lineage.downstreams else []
            }
        except Exception as e:
            return {"error": str(e), "table": table_name}
    
    def extract_column_lineage(self, table_name: str) -> List[ColumnNode]:
        try:
            lineage = self.client.lineage.get_column_lineage(
                table_name=table_name
            )
            columns = []
            if lineage and hasattr(lineage, 'columns'):
                for col in lineage.columns:
                    upstream = [u.name for u in col.upstreams] if hasattr(col, 'upstreams') else []
                    columns.append(ColumnNode(
                        name=col.name,
                        table=table_name,
                        data_type=col.data_type if hasattr(col, 'data_type') else 'unknown',
                        upstream_columns=upstream
                    ))
            return columns
        except Exception:
            return []
    
    def extract_workspace_lineage(self, workspace_id: str) -> Dict:
        tables = self.client.tables.list(catalog_name="main")
        lineage_data = {"tables": [], "edges": []}
        for table in tables:
            table_lineage = self.extract_table_lineage(table.full_name)
            if "error" not in table_lineage:
                lineage_data["tables"].append({
                    "id": table.full_name,
                    "name": table.name,
                    "metadata": {"workspace": workspace_id}
                })
                for upstream in table_lineage.get("upstream", []):
                    lineage_data["edges"].append({
                        "source": upstream,
                        "target": table.full_name
                    })
        return lineage_data
