"""Extract lineage from Databricks assets."""
import os
import re
from typing import Dict, List, Set, Any, Optional, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError
import json


class DatabricksLineageExtractor:
    """Extract lineage from Databricks notebooks, jobs, and DLT pipelines."""

    def __init__(self, workspace_url: str, token: str,
                 http_client: Optional[Callable] = None):
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self._http_client = http_client

    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make authenticated request to Databricks API."""
        if self._http_client:
            return self._http_client(endpoint)
        
        url = f"{self.workspace_url}/api/2.0/{endpoint}"
        req = Request(url)
        req.add_header('Authorization', f'Bearer {self.token}')
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())

    def _parse_sql_lineage(self, sql: str) -> Dict[str, List[str]]:
        """Extract table references from SQL."""
        inputs, outputs = set(), set()
        sql_clean = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
        
        for match in re.finditer(r'\b(?:FROM|JOIN)\s+([\w.]+)', sql_clean,
                                  re.IGNORECASE):
            inputs.add(match.group(1))
        
        for match in re.finditer(r'\b(?:INTO|TABLE)\s+([\w.]+)', sql_clean,
                                  re.IGNORECASE):
            outputs.add(match.group(1))
        
        return {'inputs': list(inputs), 'outputs': list(outputs)}

    def extract_lineage(self) -> Dict[str, Any]:
        """Extract complete lineage graph."""
        lineage = {'nodes': [], 'edges': []}
        node_ids = set()
        
        try:
            jobs_data = self._make_request('jobs/list')
            for job in jobs_data.get('jobs', []):
                job_id = f"job_{job['job_id']}"
                if job_id not in node_ids:
                    lineage['nodes'].append({
                        'id': job_id, 'type': 'job',
                        'name': job.get('settings', {}).get('name', 'Unknown')
                    })
                    node_ids.add(job_id)
        except (URLError, KeyError):
            pass
        
        try:
            notebooks = self._make_request('workspace/list?path=/')
            for nb in notebooks.get('objects', []):
                if nb.get('object_type') == 'NOTEBOOK':
                    nb_id = f"notebook_{nb['path']}"
                    if nb_id not in node_ids:
                        lineage['nodes'].append({
                            'id': nb_id, 'type': 'notebook',
                            'name': nb['path']
                        })
                        node_ids.add(nb_id)
        except (URLError, KeyError):
            pass
        
        return lineage
