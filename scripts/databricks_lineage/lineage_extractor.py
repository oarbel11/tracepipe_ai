import requests
from databricks import sql
from typing import Dict, List, Set
import logging

logger = logging.getLogger(__name__)


class DatabricksLineageExtractor:
    def __init__(self, host: str, token: str, http_path: str = None):
        self.host = host.rstrip('/')
        self.token = token
        self.http_path = http_path
        self.headers = {'Authorization': f'Bearer {token}'}

    def extract_table_lineage(self) -> List[Dict]:
        """Extract Unity Catalog table lineage."""
        if not self.http_path:
            return []
        
        conn = sql.connect(
            server_hostname=self.host.replace('https://', ''),
            http_path=self.http_path,
            access_token=self.token
        )
        cursor = conn.cursor()
        
        query = """
        SELECT table_catalog, table_schema, table_name, 
               upstream_table_catalog, upstream_table_schema, upstream_table_name
        FROM system.access.table_lineage
        WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        lineage = []
        for row in results:
            lineage.append({
                'source_catalog': row[3], 'source_schema': row[4], 'source_table': row[5],
                'target_catalog': row[0], 'target_schema': row[1], 'target_table': row[2],
                'type': 'table_to_table'
            })
        return lineage

    def extract_jobs(self) -> List[Dict]:
        """Extract Databricks jobs metadata."""
        url = f'{self.host}/api/2.1/jobs/list'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        jobs = response.json().get('jobs', [])
        return [{'job_id': j['job_id'], 'name': j['settings']['name'], 
                 'tasks': j['settings'].get('tasks', [])} for j in jobs]

    def extract_notebooks(self) -> List[Dict]:
        """Extract notebook paths."""
        url = f'{self.host}/api/2.0/workspace/list'
        response = requests.get(url, headers=self.headers, params={'path': '/'})
        response.raise_for_status()
        
        objects = response.json().get('objects', [])
        return [{'path': obj['path'], 'type': obj['object_type']} 
                for obj in objects if obj['object_type'] == 'NOTEBOOK']

    def extract_dlt_pipelines(self) -> List[Dict]:
        """Extract DLT pipeline definitions."""
        url = f'{self.host}/api/2.0/pipelines'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        pipelines = response.json().get('statuses', [])
        return [{'pipeline_id': p['pipeline_id'], 'name': p.get('name', ''),
                 'state': p.get('state', '')} for p in pipelines]
