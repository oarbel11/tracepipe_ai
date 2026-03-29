import json
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod


class ExternalConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.system_name = config.get('name', 'unknown')

    @abstractmethod
    def extract_lineage(self) -> List[Dict[str, Any]]:
        pass


class TableauConnector(ExternalConnector):
    def extract_lineage(self) -> List[Dict[str, Any]]:
        lineage = []
        sources = self.config.get('data_sources', [])
        dashboards = self.config.get('dashboards', [])
        for dash in dashboards:
            for src in sources:
                lineage.append({
                    'source': src,
                    'target': dash,
                    'system': 'tableau',
                    'type': 'visualization'
                })
        return lineage


class PowerBIConnector(ExternalConnector):
    def extract_lineage(self) -> List[Dict[str, Any]]:
        lineage = []
        datasets = self.config.get('datasets', [])
        reports = self.config.get('reports', [])
        for report in reports:
            for dataset in datasets:
                lineage.append({
                    'source': dataset,
                    'target': report,
                    'system': 'powerbi',
                    'type': 'report'
                })
        return lineage


class SnowflakeConnector(ExternalConnector):
    def extract_lineage(self) -> List[Dict[str, Any]]:
        lineage = []
        tables = self.config.get('tables', [])
        for table in tables:
            lineage.append({
                'source': table.get('upstream', []),
                'target': table.get('name'),
                'system': 'snowflake',
                'type': 'table'
            })
        return lineage


class ExternalConnectorRegistry:
    def __init__(self):
        self.connectors = {
            'tableau': TableauConnector,
            'powerbi': PowerBIConnector,
            'snowflake': SnowflakeConnector,
        }

    def get_connector(self, system_type: str, config: Dict[str, Any]) -> Optional[ExternalConnector]:
        connector_class = self.connectors.get(system_type.lower())
        return connector_class(config) if connector_class else None

    def register_connector(self, system_type: str, connector_class: type):
        self.connectors[system_type.lower()] = connector_class
