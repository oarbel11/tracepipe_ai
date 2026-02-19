"""
╔══════════════════════════════════════════════════════════════════╗
║                    IMPACT ANALYSIS MAPPER                        ║
╚══════════════════════════════════════════════════════════════════╝

Maps downstream impact of code changes using lineage metadata.

USAGE:
    from peer_review.impact_analysis import ImpactAnalysisMapper
    from peer_review.semantic_delta import SemanticDeltaExtractor
    
    extractor = SemanticDeltaExtractor()
    delta = extractor.extract_from_git()
    
    mapper = ImpactAnalysisMapper()
    impact = mapper.map_impact(delta.modified_elements)
    
    for node in impact.impacted_nodes:
        print(f"{node['table']} - {node['criticality']}")
"""

import sys
from pathlib import Path
from typing import List, Dict, Set, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import networkx as nx
except ImportError:
    nx = None

from scripts.debug_engine import DebugEngine
from config.db_config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class ImpactedNode:
    """Represents a table/view impacted by the change."""
    table: str
    node_type: str  # 'intermediate', 'leaf_node'
    relationship: str  # 'direct_dependency', 'aggregated_dependency', 'filtered_dependency'
    distance: int  # Hops from the changed element
    criticality: str  # 'HIGH', 'MEDIUM', 'LOW'
    downstream_count: int  # Number of tables depending on this one
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class ImpactAnalysis:
    """Result of downstream impact analysis."""
    impacted_nodes: List[Dict[str, Any]]
    total_impact: int
    leaf_nodes: List[str]  # Reports, dashboards, final aggregates
    max_distance: int
    criticality_breakdown: Dict[str, int]  # Count by criticality level
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IMPACT ANALYSIS MAPPER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ImpactAnalysisMapper:
    """
    Maps downstream impact of code changes using lineage metadata.
    
    Integrates with DebugEngine to trace dependencies through the data stack.
    """
    
    def __init__(self, debug_engine: Optional[DebugEngine] = None):
        """
        Initialize mapper.
        
        Args:
            debug_engine: Optional DebugEngine instance (creates one if not provided)
        """
        self.engine = debug_engine
        if not self.engine:
            try:
                config = load_config()
                self.engine = DebugEngine(config=config)
            except Exception as e:
                logger.error(f"Could not initialize DebugEngine: {e}")
                self.engine = None
        
        self.graph = None
        if nx:
            self.graph = nx.DiGraph()
    
    def map_impact(self, modified_elements: List[str], max_depth: int = 10) -> ImpactAnalysis:
        """
        Map the downstream impact of modified elements.
        
        Args:
            modified_elements: List of table.column or table strings
            max_depth: Maximum depth to traverse
        
        Returns:
            ImpactAnalysis object with impact details
        """
        if not self.engine:
            logger.error("DebugEngine not available")
            return BlastRadius(
                impacted_nodes=[],
                total_impact=0,
                leaf_nodes=[],
                max_distance=0,
                criticality_breakdown={}
            )
        
        # Extract unique tables from modified elements
        tables = self._extract_tables(modified_elements)
        logger.info(f"Analyzing downstream impact for tables: {tables}")
        
        # Build dependency graph
        all_impacted = set()
        distance_map = {}  # table -> distance from change
        
        for table in tables:
            impacted = self._trace_downstream(table, max_depth)
            all_impacted.update(impacted.keys())
            
            # Track minimum distance to each impacted table
            for impacted_table, distance in impacted.items():
                if impacted_table not in distance_map or distance < distance_map[impacted_table]:
                    distance_map[impacted_table] = distance
        
        # Classify and score impacted nodes
        impacted_nodes = []
        leaf_nodes = []
        
        for table in all_impacted:
            node_info = self._classify_node(table, distance_map[table])
            impacted_nodes.append(node_info.to_dict())
            
            if node_info.node_type == 'leaf_node':
                leaf_nodes.append(table)
        
        # Sort by criticality and distance
        impacted_nodes.sort(
            key=lambda x: (
                {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}[x['criticality']],
                x['distance']
            )
        )
        
        # Calculate criticality breakdown
        criticality_breakdown = defaultdict(int)
        for node in impacted_nodes:
            criticality_breakdown[node['criticality']] += 1
        
        return ImpactAnalysis(
            impacted_nodes=impacted_nodes,
            total_impact=len(all_impacted),
            leaf_nodes=leaf_nodes,
            max_distance=max(distance_map.values()) if distance_map else 0,
            criticality_breakdown=dict(criticality_breakdown)
        )
    
    def _extract_tables(self, modified_elements: List[str]) -> Set[str]:
        """Extract unique table names from modified elements."""
        tables = set()
        
        # Build a set of known table names from the DB (schema.table format)
        known_tables = set()
        if self.engine:
            try:
                for t in self.engine.list_tables():
                    known_tables.add(f"{t['table_schema']}.{t['table_name']}")
            except:
                pass
        
        for element in modified_elements:
            if '.' in element:
                parts = element.split('.')
                if len(parts) == 2:
                    # Check if it's a known schema.table
                    if element in known_tables:
                        tables.add(element)
                    else:
                        # Assume it's table.column, take first part
                        tables.add(parts[0])
                elif len(parts) == 3:
                    # schema.table.column
                    tables.add(f"{parts[0]}.{parts[1]}")
            else:
                tables.add(element)
        
        return tables
    
    def _trace_downstream(self, table: str, max_depth: int) -> Dict[str, int]:
        """
        Trace all downstream dependencies for a table.
        
        Args:
            table: Source table name
            max_depth: Maximum depth to traverse
        
        Returns:
            Dictionary mapping table -> distance
        """
        if not self.engine:
            return {}
        
        visited = {}  # table -> distance
        queue = [(table, 0)]  # (table, distance)
        
        while queue:
            current_table, distance = queue.pop(0)
            
            if distance > max_depth:
                continue
            
            if current_table in visited:
                continue
            
            visited[current_table] = distance
            
            try:
                # Get downstream tables
                downstream = self.engine.get_downstream_tables(current_table)
                
                for child_table in downstream:
                    if child_table not in visited:
                        queue.append((child_table, distance + 1))
            
            except Exception as e:
                logger.debug(f"Could not get downstream for {current_table}: {e}")
        
        # Remove the source table from results
        visited.pop(table, None)
        
        return visited
    
    def _classify_node(self, table: str, distance: int) -> ImpactedNode:
        """
        Classify an impacted node and calculate criticality.
        
        Args:
            table: Table name
            distance: Distance from changed element
        
        Returns:
            ImpactedNode with classification
        """
        # Determine if this is a leaf node (no downstream dependencies)
        try:
            downstream = self.engine.get_downstream_tables(table) if self.engine else []
            is_leaf = len(downstream) == 0
            downstream_count = len(downstream)
        except:
            is_leaf = False
            downstream_count = 0
        
        # Classify relationship type
        # (This is simplified - could be enhanced with actual lineage analysis)
        relationship = self._determine_relationship(table)
        
        # Calculate criticality
        criticality = self._calculate_criticality(
            is_leaf=is_leaf,
            distance=distance,
            downstream_count=downstream_count,
            table=table
        )
        
        return ImpactedNode(
            table=table,
            node_type='leaf_node' if is_leaf else 'intermediate',
            relationship=relationship,
            distance=distance,
            criticality=criticality,
            downstream_count=downstream_count
        )
    
    def _determine_relationship(self, table: str) -> str:
        """
        Determine the relationship type.
        
        This is a simplified implementation. Could be enhanced by
        analyzing the actual SQL to detect aggregations, filters, etc.
        """
        # Check table naming conventions for hints
        table_lower = table.lower()
        
        if 'agg' in table_lower or 'sum' in table_lower or 'total' in table_lower:
            return 'aggregated_dependency'
        elif 'filter' in table_lower or 'active' in table_lower:
            return 'filtered_dependency'
        else:
            return 'direct_dependency'
    
    def _calculate_criticality(
        self,
        is_leaf: bool,
        distance: int,
        downstream_count: int,
        table: str
    ) -> str:
        """
        Calculate criticality score for a node.
        
        Factors:
        - Leaf nodes (reports, dashboards) are more critical
        - Closer to change = more critical
        - More downstream dependencies = more critical
        """
        score = 0
        
        # Leaf nodes get +3
        if is_leaf:
            score += 3
        
        # Distance penalty (closer = more critical)
        if distance <= 1:
            score += 3
        elif distance <= 2:
            score += 2
        elif distance <= 3:
            score += 1
        
        # Downstream count bonus
        if downstream_count > 5:
            score += 2
        elif downstream_count > 2:
            score += 1
        
        # Check for critical keywords in table name
        table_lower = table.lower()
        critical_keywords = ['dashboard', 'report', 'gold', 'prod', 'final', 'revenue', 'sales']
        if any(keyword in table_lower for keyword in critical_keywords):
            score += 2
        
        # Map score to criticality level
        if score >= 6:
            return 'HIGH'
        elif score >= 3:
            return 'MEDIUM'
        else:
            return 'LOW'


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI / Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    
    # Test with sample data
    mapper = ImpactAnalysisMapper()
    
    # Example: a Silver layer table was modified
    modified_elements = ['silver.orders']
    
    impact = mapper.map_impact(modified_elements)
    
    print("\n" + "="*70)
    print("IMPACT ANALYSIS RESULTS")
    print("="*70)
    print(json.dumps(impact.to_dict(), indent=2))
