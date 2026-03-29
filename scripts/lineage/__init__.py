"""
Cross-System Lineage Integration

Extends Unity Catalog lineage with external system data flows.
"""

from .external_connectors import ExternalConnectorRegistry
from .lineage_stitcher import LineageStitcher
from .graph_builder import LineageGraphBuilder

__all__ = [
    'ExternalConnectorRegistry',
    'LineageStitcher',
    'LineageGraphBuilder',
]
