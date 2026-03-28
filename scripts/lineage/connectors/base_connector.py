"""Base connector interface for lineage extraction."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseConnector(ABC):
    """Abstract base class for lineage connectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connector_type = "base"

    @abstractmethod
    def extract_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage information from the source/sink."""
        pass

    @abstractmethod
    def get_entities(self) -> List[str]:
        """Get list of entities (tables, topics, reports) from source."""
        pass

    def validate_config(self) -> bool:
        """Validate connector configuration."""
        return bool(self.config)
