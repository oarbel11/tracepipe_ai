"""Base connector interface for external lineage systems."""
from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseLineageConnector(ABC):
    """Abstract base class for lineage connectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.system_name = self.__class__.__name__.replace("Connector", "")

    @abstractmethod
    def fetch_lineage(self) -> Dict[str, Any]:
        """Fetch lineage from external system.
        
        Returns:
            Dict with 'nodes' and 'edges' keys
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate connection to external system."""
        pass
