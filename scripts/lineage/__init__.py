"""
Cross-Platform Lineage Module

Unifies lineage across Unity Catalog and external data sources.
"""

from .lineage_graph import LineageGraphBuilder
from .connector_registry import ConnectorRegistry
from .lineage_stitcher import LineageStitcher

__all__ = [
    'LineageGraphBuilder',
    'ConnectorRegistry',
    'LineageStitcher',
]
