"""Registry for managing external data source and sink connectors."""

from typing import Dict, Optional, Any, List


class ConnectorRegistry:
    """Manages registration and retrieval of lineage connectors."""

    def __init__(self):
        self._connectors: Dict[str, Any] = {}

    def register(self, name: str, connector: Any) -> None:
        """Register a connector by name."""
        self._connectors[name] = connector

    def get(self, name: str) -> Optional[Any]:
        """Retrieve a connector by name."""
        return self._connectors.get(name)

    def list_connectors(self) -> List[str]:
        """List all registered connector names."""
        return list(self._connectors.keys())

    def unregister(self, name: str) -> bool:
        """Unregister a connector by name."""
        if name in self._connectors:
            del self._connectors[name]
            return True
        return False
