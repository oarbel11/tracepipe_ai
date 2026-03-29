from typing import Dict, Any, Optional
import json


class ConnectorConfig:
    """Configuration for external system connectors."""

    def __init__(self, config_path: Optional[str] = None):
        self.connectors: Dict[str, Dict[str, Any]] = {}
        if config_path:
            self.load_from_file(config_path)

    def add_connector(self, name: str, connector_type: str,
                      connection_params: Dict[str, Any]) -> None:
        """Add a connector configuration."""
        self.connectors[name] = {
            'type': connector_type,
            'params': connection_params,
            'enabled': True
        }

    def get_connector(self, name: str) -> Optional[Dict[str, Any]]:
        """Get connector configuration by name."""
        return self.connectors.get(name)

    def list_connectors(self) -> list:
        """List all configured connectors."""
        return list(self.connectors.keys())

    def remove_connector(self, name: str) -> bool:
        """Remove a connector configuration."""
        if name in self.connectors:
            del self.connectors[name]
            return True
        return False

    def load_from_file(self, path: str) -> None:
        """Load configuration from a JSON file."""
        with open(path, 'r') as f:
            data = json.load(f)
            self.connectors = data.get('connectors', {})

    def save_to_file(self, path: str) -> None:
        """Save configuration to a JSON file."""
        with open(path, 'w') as f:
            json.dump({'connectors': self.connectors}, f, indent=2)
