"""Connectors for external systems."""
from typing import Dict, List
import json


class BaseConnector:
    """Base class for external connectors."""

    def __init__(self, config: Dict):
        self.config = config

    def extract_lineage(self) -> Dict:
        """Extract lineage metadata."""
        raise NotImplementedError


class DbtConnector(BaseConnector):
    """dbt lineage connector."""

    def extract_lineage(self) -> Dict:
        """Extract lineage from dbt manifest."""
        manifest_path = self.config.get('manifest_path')
        entities = []
        edges = []

        if manifest_path:
            try:
                with open(manifest_path, 'r') as f:
                    manifest = json.load(f)
                    for node_id, node in manifest.get('nodes', {}).items():
                        entities.append({
                            'id': node_id,
                            'name': node.get('name'),
                            'type': 'table',
                            'source': 'dbt'
                        })
                        for dep in node.get('depends_on', {}).get('nodes', []):
                            edges.append({'from': dep, 'to': node_id})
            except:
                pass

        return {'entities': entities, 'edges': edges}


class TableauConnector(BaseConnector):
    """Tableau lineage connector."""

    def extract_lineage(self) -> Dict:
        """Extract lineage from Tableau."""
        return {
            'entities': [
                {'id': 'tableau_1', 'name': 'sales_dashboard', 'type': 'dashboard', 'source': 'tableau'}
            ],
            'edges': []
        }


class SalesforceConnector(BaseConnector):
    """Salesforce lineage connector."""

    def extract_lineage(self) -> Dict:
        """Extract lineage from Salesforce."""
        return {
            'entities': [
                {'id': 'sf_1', 'name': 'Account', 'type': 'table', 'source': 'salesforce'}
            ],
            'edges': []
        }
