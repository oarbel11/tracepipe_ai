from typing import List, Optional
from dataclasses import dataclass


@dataclass
class TableInfo:
    catalog_name: str
    schema_name: str
    name: str
    table_type: str
    full_name: str


@dataclass
class ColumnLineage:
    name: str
    upstream_columns: List = None


@dataclass
class TableLineage:
    table_name: str
    upstream_tables: List = None


class TablesAPI:
    def list(self, catalog_name: str, schema_name: str):
        return []

    def get(self, full_name: str) -> Optional[TableInfo]:
        parts = full_name.split(".")
        if len(parts) == 3:
            return TableInfo(
                catalog_name=parts[0],
                schema_name=parts[1],
                name=parts[2],
                table_type="MANAGED",
                full_name=full_name
            )
        return None


class LineageAPI:
    def get_table_lineage(self, table_name: str) -> Optional[TableLineage]:
        return TableLineage(table_name=table_name, upstream_tables=[])


class WorkspaceClient:
    def __init__(self):
        self.tables = TablesAPI()
        self.lineage = LineageAPI()
