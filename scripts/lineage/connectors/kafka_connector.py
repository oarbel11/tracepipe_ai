"""Kafka connector for lineage extraction."""

from typing import List, Dict, Any
from .base_connector import BaseConnector


class KafkaConnector(BaseConnector):
    """Extracts lineage from Kafka topics."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connector_type = "kafka"
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")

    def extract_lineage(self) -> List[Dict[str, Any]]:
        """Extract lineage from Kafka topics."""
        topics = self.get_entities()
        lineage = []
        for topic in topics:
            lineage.append({
                "entity_id": f"kafka://{topic}",
                "entity_type": "kafka_topic",
                "metadata": {
                    "topic": topic,
                    "bootstrap_servers": self.bootstrap_servers
                }
            })
        return lineage

    def get_entities(self) -> List[str]:
        """Get list of Kafka topics."""
        return self.config.get("topics", [])
