import networkx as nx
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod


class BaseConnector(ABC):
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config

    @abstractmethod
    def extract_lineage(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_node_type(self) -> str:
        pass


class ConnectorRegistry:
    def __init__(self):
        self._connectors: Dict[str, BaseConnector] = {}

    def register(self, connector_id: str, connector: BaseConnector):
        self._connectors[connector_id] = connector

    def unregister(self, connector_id: str):
        if connector_id in self._connectors:
            del self._connectors[connector_id]

    def get_connector(self, connector_id: str) -> Optional[BaseConnector]:
        return self._connectors.get(connector_id)

    def list_connectors(self) -> List[str]:
        return list(self._connectors.keys())

    def extract_all_lineage(self) -> Dict[str, List[Dict[str, Any]]]:
        lineage_data = {}
        for conn_id, connector in self._connectors.items():
            try:
                lineage_data[conn_id] = connector.extract_lineage()
            except Exception as e:
                lineage_data[conn_id] = []
        return lineage_data
