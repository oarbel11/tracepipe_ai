from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime


@dataclass
class ConnectorConfig:
    connector_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    credentials: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class LineageEdge:
    source_system: str
    source_asset: str
    target_system: str
    target_asset: str
    operation_type: str
    timestamp: datetime
    metadata: Dict[str, Any]


class BaseConnector(ABC):
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def discover_assets(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def extract_lineage(self) -> List[LineageEdge]:
        pass

    @abstractmethod
    def infer_relationships(self, databricks_catalog: Dict) -> List[LineageEdge]:
        pass

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def test_connection(self) -> Dict[str, Any]:
        try:
            success = self.connect()
            self.disconnect()
            return {"status": "success" if success else "failed"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
