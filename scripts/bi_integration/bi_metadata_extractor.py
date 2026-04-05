import json
import re
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

@dataclass
class BIMetric:
    name: str
    dashboard: str
    platform: str
    definition: str
    upstream_tables: List[str]
    upstream_columns: List[str]

class BIMetadataExtractor:
    def __init__(self, platform: str):
        self.platform = platform.lower()
        self.supported = ['tableau', 'powerbi', 'looker']
        if self.platform not in self.supported:
            raise ValueError(f"Platform {platform} not supported")

    def extract_metadata(self, workspace: str, credentials: Dict) -> List[BIMetric]:
        if self.platform == 'tableau':
            return self._extract_tableau(workspace, credentials)
        elif self.platform == 'powerbi':
            return self._extract_powerbi(workspace, credentials)
        elif self.platform == 'looker':
            return self._extract_looker(workspace, credentials)

    def _extract_tableau(self, workspace: str, credentials: Dict) -> List[BIMetric]:
        metrics = []
        mock_dashboards = [
            {'name': 'Sales Dashboard', 'metrics': [
                {'name': 'Total Revenue', 'definition': 'SUM([sales_amount])', 
                 'tables': ['companies_data.main.sales'], 'columns': ['sales_amount']}
            ]},
            {'name': 'Customer Dashboard', 'metrics': [
                {'name': 'Active Customers', 'definition': 'COUNT(DISTINCT [customer_id])',
                 'tables': ['companies_data.main.customers'], 'columns': ['customer_id']}
            ]}
        ]
        for dash in mock_dashboards:
            for m in dash['metrics']:
                metrics.append(BIMetric(
                    name=m['name'], dashboard=dash['name'], platform='tableau',
                    definition=m['definition'], upstream_tables=m['tables'],
                    upstream_columns=m['columns']
                ))
        return metrics

    def _extract_powerbi(self, workspace: str, credentials: Dict) -> List[BIMetric]:
        return []

    def _extract_looker(self, workspace: str, credentials: Dict) -> List[BIMetric]:
        return []

    def parse_sql_from_definition(self, definition: str) -> Dict:
        tables = re.findall(r'FROM\s+([\w\.]+)', definition, re.IGNORECASE)
        columns = re.findall(r'\[(\w+)\]', definition)
        return {'tables': tables, 'columns': columns}
