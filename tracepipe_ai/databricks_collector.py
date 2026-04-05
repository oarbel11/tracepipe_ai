from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os


class DatabricksLineageCollector:
    def __init__(self, workspace_url: str = None, token: str = None):
        self.workspace_url = workspace_url or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")
        if self.workspace_url and self.token:
            self.client = WorkspaceClient(
                host=self.workspace_url,
                token=self.token
            )
        else:
            self.client = None

    def collect_lineage_events(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        if not self.client:
            return []
        
        events = []
        try:
            tables = self.client.tables.list(
                catalog_name="*",
                schema_name="*"
            )
            for table in tables:
                if hasattr(table, 'table_id'):
                    event = {
                        "id": f"{table.table_id}_{int(datetime.now().timestamp())}",
                        "event_type": "table_access",
                        "source_table": table.full_name,
                        "target_table": None,
                        "timestamp": datetime.now(),
                        "metadata": {
                            "catalog": table.catalog_name,
                            "schema": table.schema_name,
                            "table": table.name
                        }
                    }
                    events.append(event)
        except Exception as e:
            pass
        
        return events

    def is_configured(self) -> bool:
        return self.client is not None
