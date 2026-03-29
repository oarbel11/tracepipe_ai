"""Script to orchestrate cross-system lineage mapping."""

from typing import List
from connectors import (
    ConnectorConfig,
    BaseConnector,
    KafkaConnector,
    S3Connector,
    ExternalDatabaseConnector,
    LineageInferenceEngine
)


class LineageMapper:
    """Orchestrates lineage mapping across systems."""

    def __init__(self):
        self.connectors: List[BaseConnector] = []
        self.engine = LineageInferenceEngine()

    def add_connector(self, connector: BaseConnector) -> None:
        """Add a connector to the mapper."""
        self.connectors.append(connector)

    def map_lineage(self) -> LineageInferenceEngine:
        """Map lineage across all connectors."""
        for connector in self.connectors:
            if connector.connect():
                edges = connector.extract_lineage()
                self.engine.add_edges(edges)
                connector.disconnect()
        return self.engine

    def get_engine(self) -> LineageInferenceEngine:
        """Get the lineage inference engine."""
        return self.engine


def create_connector(connector_type: str, config: ConnectorConfig) -> BaseConnector:
    """Factory function to create connectors."""
    if connector_type == 'kafka':
        return KafkaConnector(config)
    elif connector_type == 's3':
        return S3Connector(config)
    elif connector_type == 'database':
        return ExternalDatabaseConnector(config)
    else:
        raise ValueError(f"Unknown connector type: {connector_type}")


if __name__ == '__main__':
    mapper = LineageMapper()
    
    kafka_config = ConnectorConfig(
        name='kafka-prod',
        connector_type='kafka',
        credentials={},
        connection_params={'bootstrap_servers': 'localhost:9092'}
    )
    
    kafka_connector = create_connector('kafka', kafka_config)
    mapper.add_connector(kafka_connector)
    
    engine = mapper.map_lineage()
    print(f"Mapped {len(engine.get_all_edges())} lineage edges")
