try:
    from databricks.sdk import WorkspaceClient
    HAS_DATABRICKS_SDK = True
except ImportError:
    HAS_DATABRICKS_SDK = False
    WorkspaceClient = None

from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class ColumnNode:
    name: str
    table: str
    data_type: Optional[str] = None

class LineageExtractor:
    def __init__(self, workspace_url: Optional[str] = None, token: Optional[str] = None):
        self.workspace_url = workspace_url
        self.token = token
        self.client = None
        if HAS_DATABRICKS_SDK and workspace_url and token:
            self.client = WorkspaceClient(host=workspace_url, token=token)
    
    def extract_unity_catalog_lineage(self, catalog: str, schema: str) -> Dict[str, Any]:
        lineage_data = {
            "tables": [],
            "relationships": []
        }
        if not self.client:
            return lineage_data
        try:
            tables = self.client.tables.list(catalog_name=catalog, schema_name=schema)
            for table in tables:
                lineage_data["tables"].append({
                    "name": table.name,
                    "full_name": table.full_name,
                    "type": table.table_type
                })
        except Exception:
            pass
        return lineage_data
    
    def extract_table_lineage(self, table_name: str) -> Dict[str, Any]:
        if not self.client:
            return {"upstream": [], "downstream": []}
        try:
            lineage = self.client.lineage.get_table_lineage(table_name=table_name)
            return {
                "upstream": [u.table_info.name for u in lineage.upstreams] if lineage.upstreams else [],
                "downstream": [d.table_info.name for d in lineage.downstreams] if lineage.downstreams else []
            }
        except Exception:
            return {"upstream": [], "downstream": []}
    
    def parse_spark_sql(self, sql: str) -> List[ColumnNode]:
        columns = []
        sql_upper = sql.upper()
        if "SELECT" in sql_upper:
            select_idx = sql_upper.find("SELECT")
            from_idx = sql_upper.find("FROM")
            if select_idx != -1 and from_idx != -1:
                col_part = sql[select_idx + 6:from_idx].strip()
                for col in col_part.split(","):
                    col = col.strip()
                    if col and col != "*":
                        col_name = col.split()[-1] if " AS " in col.upper() else col
                        columns.append(ColumnNode(name=col_name, table=""))
        return columns
