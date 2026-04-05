from abc import ABC, abstractmethod
from typing import List, Dict, Any
import networkx as nx

class LineageNode:
    def __init__(self, identifier: str, node_type: str, system: str, metadata: Dict[str, Any] = None):
        self.identifier = identifier
        self.node_type = node_type
        self.system = system
        self.metadata = metadata or {}

    def to_dict(self):
        return {
            'identifier': self.identifier,
            'node_type': self.node_type,
            'system': self.system,
            'metadata': self.metadata
        }

class LineageEdge:
    def __init__(self, source: str, target: str, edge_type: str = 'derives_from', metadata: Dict[str, Any] = None):
        self.source = source
        self.target = target
        self.edge_type = edge_type
        self.metadata = metadata or {}

class BaseLineageConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.system_name = self.__class__.__name__.replace('Connector', '')

    @abstractmethod
    def extract_lineage(self) -> nx.DiGraph:
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        pass

class ConnectorRegistry:
    _connectors = {}

    @classmethod
    def register(cls, name: str, connector_class):
        cls._connectors[name] = connector_class

    @classmethod
    def get_connector(cls, name: str, config: Dict[str, Any]):
        if name not in cls._connectors:
            raise ValueError(f"Connector '{name}' not registered")
        return cls._connectors[name](config)

    @classmethod
    def list_connectors(cls):
        return list(cls._connectors.keys())

__all__ = ['LineageNode', 'LineageEdge', 'BaseLineageConnector', 'ConnectorRegistry']
