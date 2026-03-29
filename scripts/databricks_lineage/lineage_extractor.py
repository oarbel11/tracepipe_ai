"""Extract lineage from Databricks assets."""
import os
from typing import Dict, List, Any
from databricks.sdk import WorkspaceClient


class DatabricksLineageExtractor:
    """Extract lineage from Databricks notebooks, jobs, and tables."""

    def __init__(self, host: str = None, token: str = None):
        self.host = host or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")
        self.client = WorkspaceClient(host=self.host, token=self.token)

    def extract_jobs_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from Databricks jobs."""
        jobs_lineage = []
        try:
            jobs = list(self.client.jobs.list())
            for job in jobs:
                job_detail = self.client.jobs.get(job.job_id)
                lineage_entry = {
                    "type": "job",
                    "id": str(job.job_id),
                    "name": job.settings.name if job.settings else "",
                    "tasks": [],
                }
                if job_detail.settings and job_detail.settings.tasks:
                    for task in job_detail.settings.tasks:
                        task_info = {"task_key": task.task_key}
                        if task.notebook_task:
                            task_info["notebook"] = task.notebook_task.notebook_path
                        lineage_entry["tasks"].append(task_info)
                jobs_lineage.append(lineage_entry)
        except Exception as e:
            print(f"Error extracting jobs: {e}")
        return jobs_lineage

    def extract_notebooks_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from Databricks notebooks."""
        notebooks_lineage = []
        try:
            objects = list(self.client.workspace.list("/", recursive=True))
            for obj in objects:
                if obj.object_type.name == "NOTEBOOK":
                    notebooks_lineage.append({
                        "type": "notebook",
                        "path": obj.path,
                        "id": obj.path,
                    })
        except Exception as e:
            print(f"Error extracting notebooks: {e}")
        return notebooks_lineage

    def extract_tables_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from Unity Catalog tables."""
        tables_lineage = []
        try:
            catalogs = list(self.client.catalogs.list())
            for catalog in catalogs:
                schemas = list(self.client.schemas.list(catalog.name))
                for schema in schemas:
                    tables = list(self.client.tables.list(
                        catalog.name, schema.name))
                    for table in tables:
                        tables_lineage.append({
                            "type": "table",
                            "id": table.full_name,
                            "name": table.name,
                            "catalog": catalog.name,
                            "schema": schema.name,
                        })
        except Exception as e:
            print(f"Error extracting tables: {e}")
        return tables_lineage
