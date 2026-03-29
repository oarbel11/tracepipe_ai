"""Base connector interface for cross-system lineage."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Any


@dataclass
class ConnectorConfig:
    """Configuration for external system connectors."""
    name: str
    connector_type: str
    credentials: Dict[str, str]
    connection_params: Dict[str, Any]


@dataclass
class LineageEdge:
    """Represents a lineage relationship between assets."""
    source_system: str
    source_asset: str
    target_system: str
    target_asset: str
    metadata: Optional[Dict[str, Any]] = None


class BaseConnector(ABC):
    """Base class for all external system connectors."""

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to external system."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to external system."""
        pass

    @abstractmethod
    def discover_assets(self) -> List[str]:
        """Discover available assets in external system."""
        pass

    @abstractmethod
    def extract_lineage(self) -> List[LineageEdge]:
        """Extract lineage information from external system."""
        pass

    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected
