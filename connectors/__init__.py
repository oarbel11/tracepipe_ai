"""Base connector interface and registry for external lineage sources."""
from abc import ABC, abstractmethod
from typing import Dict, List, Any


class LineageNode:
    """Represents a node in the lineage graph."""
    def __init__(self, id: str, name: str, type: str, system: str, metadata: Dict = None):
        self.id = id
        self.name = name
        self.type = type
        self.system = system
        self.metadata = metadata or {}

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "system": self.system,
            "metadata": self.metadata
        }


class LineageEdge:
    """Represents an edge in the lineage graph."""
    def __init__(self, source_id: str, target_id: str, edge_type: str = "depends_on"):
        self.source_id = source_id
        self.target_id = target_id
        self.edge_type = edge_type

    def to_dict(self):
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type
        }


class BaseLineageConnector(ABC):
    """Base class for all lineage connectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def extract_lineage(self) -> Dict[str, Any]:
        """Extract lineage data from the source system."""
        pass


class ConnectorRegistry:
    """Registry for managing lineage connectors."""
    _connectors = {}

    @classmethod
    def register(cls, name: str, connector_class):
        cls._connectors[name] = connector_class

    @classmethod
    def get_connector(cls, name: str, config: Dict[str, Any]):
        connector_class = cls._connectors.get(name)
        if not connector_class:
            raise ValueError(f"Connector '{name}' not found")
        return connector_class(config)

    @classmethod
    def list_connectors(cls):
        return list(cls._connectors.keys())
