from typing import Dict, List, Any, Optional


class LineageIntegrator:
    """Integrates lineage from external systems."""

    def __init__(self):
        self.connectors: Dict[str, Any] = {}
        self.lineage_cache: Dict[str, List[Dict[str, Any]]] = {}

    def register_connector(self, name: str, connector: Any) -> None:
        """Register an external system connector."""
        self.connectors[name] = connector

    def fetch_lineage(self, connector_name: str,
                      filters: Optional[Dict[str, Any]] = None
                      ) -> List[Dict[str, Any]]:
        """Fetch lineage data from an external system."""
        if connector_name not in self.connectors:
            raise ValueError(f"Connector '{connector_name}' not found")

        connector = self.connectors[connector_name]
        lineage = connector.get_lineage(filters or {})
        self.lineage_cache[connector_name] = lineage
        return lineage

    def get_cached_lineage(self, connector_name: str
                           ) -> Optional[List[Dict[str, Any]]]:
        """Get cached lineage data."""
        return self.lineage_cache.get(connector_name)

    def clear_cache(self, connector_name: Optional[str] = None) -> None:
        """Clear lineage cache."""
        if connector_name:
            self.lineage_cache.pop(connector_name, None)
        else:
            self.lineage_cache.clear()

    def list_connectors(self) -> List[str]:
        """List all registered connectors."""
        return list(self.connectors.keys())
