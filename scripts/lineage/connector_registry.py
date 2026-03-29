from typing import Dict, Callable, Optional, List

class ConnectorRegistry:
    def __init__(self):
        self.connectors: Dict[str, Callable] = {}
        self.metadata: Dict[str, Dict] = {}

    def register_connector(self, name: str, connector_func: Callable, 
                          metadata: Optional[Dict] = None) -> None:
        self.connectors[name] = connector_func
        self.metadata[name] = metadata or {}

    def get_connector(self, name: str) -> Optional[Callable]:
        return self.connectors.get(name)

    def list_connectors(self) -> List[str]:
        return list(self.connectors.keys())

    def extract_lineage(self, connector_name: str, config: Dict) -> Dict:
        connector = self.get_connector(connector_name)
        if not connector:
            raise ValueError(f"Connector '{connector_name}' not found")
        return connector(config)

    def get_connector_metadata(self, name: str) -> Dict:
        return self.metadata.get(name, {})
