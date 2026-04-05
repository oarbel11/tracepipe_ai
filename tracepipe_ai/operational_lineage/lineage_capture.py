"""Capture operational lineage from Databricks workloads."""

from typing import Dict, List, Any, Optional
from datetime import datetime


class LineageCapture:
    """Captures lineage metadata from Databricks sources."""

    def __init__(self, workspace_client: Optional[Any] = None):
        self.workspace_client = workspace_client
        self.lineage_records = []

    def capture_notebook_lineage(self, notebook_path: str,
                                  tables_read: List[str],
                                  tables_written: List[str]) -> Dict[str, Any]:
        """Capture lineage for a notebook execution."""
        record = {
            'type': 'notebook',
            'asset_id': notebook_path,
            'tables_read': tables_read,
            'tables_written': tables_written,
            'timestamp': datetime.now().isoformat()
        }
        self.lineage_records.append(record)
        return record

    def capture_job_lineage(self, job_id: str, job_name: str,
                            tables_read: List[str],
                            tables_written: List[str]) -> Dict[str, Any]:
        """Capture lineage for a Spark job execution."""
        record = {
            'type': 'job',
            'asset_id': job_id,
            'asset_name': job_name,
            'tables_read': tables_read,
            'tables_written': tables_written,
            'timestamp': datetime.now().isoformat()
        }
        self.lineage_records.append(record)
        return record

    def capture_dlt_lineage(self, pipeline_id: str, pipeline_name: str,
                            tables_read: List[str],
                            tables_written: List[str]) -> Dict[str, Any]:
        """Capture lineage for a Delta Live Tables pipeline."""
        record = {
            'type': 'dlt',
            'asset_id': pipeline_id,
            'asset_name': pipeline_name,
            'tables_read': tables_read,
            'tables_written': tables_written,
            'timestamp': datetime.now().isoformat()
        }
        self.lineage_records.append(record)
        return record

    def capture_dbt_lineage(self, model_name: str,
                            tables_read: List[str],
                            tables_written: List[str]) -> Dict[str, Any]:
        """Capture lineage for a dbt model execution."""
        record = {
            'type': 'dbt',
            'asset_id': model_name,
            'tables_read': tables_read,
            'tables_written': tables_written,
            'timestamp': datetime.now().isoformat()
        }
        self.lineage_records.append(record)
        return record

    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all captured lineage records."""
        return self.lineage_records
