"""Databricks lineage extractor using REST API."""

import os
import json
import re
from typing import Dict, List, Any, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError


class DatabricksLineageExtractor:
    """Extract lineage from Databricks using REST API."""

    def __init__(self, host: str, token: str):
        self.host = host.rstrip('/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def _make_request(self, endpoint: str) -> Any:
        """Make REST API request to Databricks."""
        url = f"{self.host}/api/2.0/{endpoint}"
        req = Request(url, headers=self.headers)
        try:
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode())
        except HTTPError:
            return {}

    def extract_tables(self) -> List[Dict[str, Any]]:
        """Extract table metadata."""
        result = self._make_request('unity-catalog/tables')
        tables = result.get('tables', [])
        return [{
            'id': t.get('table_id', t.get('name', 'unknown')),
            'name': t.get('full_name', t.get('name', 'unknown')),
            'type': 'table',
            'catalog': t.get('catalog_name', 'unknown'),
            'schema': t.get('schema_name', 'unknown')
        } for t in tables]

    def extract_jobs(self) -> List[Dict[str, Any]]:
        """Extract job metadata."""
        result = self._make_request('jobs/list')
        jobs = result.get('jobs', [])
        return [{
            'id': str(j.get('job_id', 'unknown')),
            'name': j.get('settings', {}).get('name', 'unknown'),
            'type': 'job'
        } for j in jobs]

    def extract_notebooks(self) -> List[Dict[str, Any]]:
        """Extract notebook metadata."""
        result = self._make_request('workspace/list?path=/')
        objects = result.get('objects', [])
        notebooks = [o for o in objects if o.get('object_type') == 'NOTEBOOK']
        return [{
            'id': n.get('object_id', n.get('path', 'unknown')),
            'name': n.get('path', 'unknown'),
            'type': 'notebook'
        } for n in notebooks]

    def extract_lineage(self) -> Dict[str, Any]:
        """Extract complete lineage graph."""
        tables = self.extract_tables()
        jobs = self.extract_jobs()
        notebooks = self.extract_notebooks()
        
        nodes = tables + jobs + notebooks
        edges = []
        
        return {'nodes': nodes, 'edges': edges}
