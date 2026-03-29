"""
Cross-System Lineage Connectors

Provides pre-built connectors and inference mechanisms for external data sources.
"""

from .base import BaseConnector, ConnectorConfig, LineageEdge
from .external_sources import (
    KafkaConnector,
    S3Connector,
    ExternalDBConnector,
    SnowflakeConnector
)
from .lineage_engine import LineageInferenceEngine

__all__ = [
    'BaseConnector',
    'ConnectorConfig',
    'LineageEdge',
    'KafkaConnector',
    'S3Connector',
    'ExternalDBConnector',
    'SnowflakeConnector',
    'LineageInferenceEngine',
]
