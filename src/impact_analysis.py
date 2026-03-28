"""Advanced Impact & Root Cause Analysis Module.

Provides functionality to analyze data lineage, identify root causes,
and simulate impact of changes across data assets.
"""

import json
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict, deque


@dataclass
class DataAsset:
    """Represents a data asset in the lineage graph."""
    asset_id: str
    asset_type: str  # table, column, view, dashboard, etc.
    name: str
    schema: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class Dependency:
    """Represents a dependency relationship between assets."""
    source_id: str
    target_id: str
    dependency_type: str  # direct, indirect, computed


@dataclass
class ImpactAnalysisResult:
    """Result of impact analysis."""
    root_asset: DataAsset
    affected_assets: List[DataAsset]
    dependency_chain: List[Dependency]
    impact_score: float
    recommendations: List[str]


@dataclass
class RootCauseAnalysisResult:
    """Result of root cause analysis."""
    affected_asset: DataAsset
    root_causes: List[DataAsset]
    dependency_chain: List[Dependency]
    confidence_score: float


class ImpactAnalysisEngine:
    """Core engine for impact and root cause analysis."""

    def __init__(self, db_connection=None):
        """Initialize the impact analysis engine.
        
        Args:
            db_connection: Optional database connection for metadata queries
        """
        self.db_connection = db_connection
        self.assets: Dict[str, DataAsset] = {}
        self.dependencies: List[Dependency] = []
        self.adjacency_list: Dict[str, List[str]] = defaultdict(list)
        self.reverse_adjacency_list: Dict[str, List[str]] = defaultdict(list)

    def load_metadata(self) -> None:
        """Load metadata from database and build lineage graph."""
        if self.db_connection is None:
            return
        
        try:
            # Load tables
            tables = self.db_connection.execute(
                "SELECT table_schema, table_name FROM information_schema.tables"
            ).fetchall()
            
            for schema, table in tables:
                asset_id = f"{schema}.{table}"
                self.assets[asset_id] = DataAsset(
                    asset_id=asset_id,
                    asset_type="table",
                    name=table,
                    schema=schema
                )
            
            # Load columns
            columns = self.db_connection.execute(
                "SELECT table_schema, table_name, column_name FROM information_schema.columns"
            ).fetchall()
            
            for schema, table, column in columns:
                asset_id = f"{schema}.{table}.{column}"
                self.assets[asset_id] = DataAsset(
                    asset_id=asset_id,
                    asset_type="column",
                    name=column,
                    schema=schema
                )
                # Create dependency from column to table
                table_id = f"{schema}.{table}"
                self._add_dependency(asset_id, table_id, "belongs_to")
        except Exception:
            # Silently handle database errors
            pass

    def _add_dependency(self, source_id: str, target_id: str, dep_type: str) -> None:
        """Add a dependency relationship."""
        dep = Dependency(source_id, target_id, dep_type)
        self.dependencies.append(dep)
        self.adjacency_list[source_id].append(target_id)
        self.reverse_adjacency_list[target_id].append(source_id)

    def analyze_downstream_impact(self, asset_id: str, max_depth: int = 10) -> ImpactAnalysisResult:
        """Analyze downstream impact of changes to an asset.
        
        Args:
            asset_id: ID of the asset to analyze
            max_depth: Maximum depth to traverse
            
        Returns:
            ImpactAnalysisResult with affected assets and recommendations
        """
        if asset_id not in self.assets:
            raise ValueError(f"Asset {asset_id} not found")
        
        root_asset = self.assets[asset_id]
        affected_assets = []
        dependency_chain = []
        visited = set()
        
        # BFS to find all downstream dependencies
        queue = deque([(asset_id, 0)])
        
        while queue:
            current_id, depth = queue.popleft()
            
            if depth > max_depth or current_id in visited:
                continue
            
            visited.add(current_id)
            
            if current_id != asset_id:
                affected_assets.append(self.assets[current_id])
            
            for next_id in self.adjacency_list.get(current_id, []):
                if next_id not in visited:
                    queue.append((next_id, depth + 1))
                    # Find the dependency
                    for dep in self.dependencies:
                        if dep.source_id == current_id and dep.target_id == next_id:
                            dependency_chain.append(dep)
                            break
        
        # Calculate impact score
        impact_score = min(len(affected_assets) / 10.0, 1.0)
        
        # Generate recommendations
        recommendations = self._generate_impact_recommendations(
            root_asset, affected_assets, impact_score
        )
        
        return ImpactAnalysisResult(
            root_asset=root_asset,
            affected_assets=affected_assets,
            dependency_chain=dependency_chain,
            impact_score=impact_score,
            recommendations=recommendations
        )

    def analyze_root_cause(self, asset_id: str, max_depth: int = 10) -> RootCauseAnalysisResult:
        """Analyze upstream dependencies to identify root causes.
        
        Args:
            asset_id: ID of the affected asset
            max_depth: Maximum depth to traverse upstream
            
        Returns:
            RootCauseAnalysisResult with potential root causes
        """
        if asset_id not in self.assets:
            raise ValueError(f"Asset {asset_id} not found")
        
        affected_asset = self.assets[asset_id]
        root_causes = []
        dependency_chain = []
        visited = set()
        
        # BFS to find all upstream dependencies
        queue = deque([(asset_id, 0)])
        
        while queue:
            current_id, depth = queue.popleft()
            
            if depth > max_depth or current_id in visited:
                continue
            
            visited.add(current_id)
            
            # Check if this is a root (no upstream dependencies)
            upstream = self.reverse_adjacency_list.get(current_id, [])
            if not upstream and current_id != asset_id:
                root_causes.append(self.assets[current_id])
            
            for prev_id in upstream:
                if prev_id not in visited:
                    queue.append((prev_id, depth + 1))
                    # Find the dependency
                    for dep in self.dependencies:
                        if dep.source_id == prev_id and dep.target_id == current_id:
                            dependency_chain.append(dep)
                            break
        
        # Calculate confidence score based on number of paths found
        confidence_score = min(len(root_causes) / 5.0, 1.0) if root_causes else 0.0
        
        return RootCauseAnalysisResult(
            affected_asset=affected_asset,
            root_causes=root_causes,
            dependency_chain=dependency_chain,
            confidence_score=confidence_score
        )

    def _generate_impact_recommendations(self, root_asset: DataAsset, 
                                        affected_assets: List[DataAsset],
                                        impact_score: float) -> List[str]:
        """Generate recommendations based on impact analysis."""
        recommendations = []
        
        if impact_score > 0.7:
            recommendations.append("HIGH RISK: This change affects many downstream assets")
            recommendations.append("Recommend thorough testing in staging environment")
            recommendations.append("Consider phased rollout with monitoring")
        elif impact_score > 0.4:
            recommendations.append("MEDIUM RISK: Moderate downstream impact detected")
            recommendations.append("Review affected dashboards and reports")
        else:
            recommendations.append("LOW RISK: Limited downstream impact")
        
        # Check for dashboards
        dashboard_count = sum(1 for a in affected_assets if a.asset_type == "dashboard")
        if dashboard_count > 0:
            recommendations.append(f"Alert: {dashboard_count} dashboards will be affected")
        
        return recommendations

    def get_asset_lineage(self, asset_id: str) -> Dict[str, Any]:
        """Get complete lineage (upstream and downstream) for an asset."""
        if asset_id not in self.assets:
            raise ValueError(f"Asset {asset_id} not found")
        
        upstream_result = self.analyze_root_cause(asset_id)
        downstream_result = self.analyze_downstream_impact(asset_id)
        
        return {
            "asset": asdict(self.assets[asset_id]),
            "upstream": {
                "count": len(upstream_result.root_causes),
                "assets": [asdict(a) for a in upstream_result.root_causes]
            },
            "downstream": {
                "count": len(downstream_result.affected_assets),
                "assets": [asdict(a) for a in downstream_result.affected_assets]
            }
        }

    def export_graph(self, format: str = "json") -> str:
        """Export the lineage graph in specified format."""
        if format == "json":
            graph_data = {
                "assets": [asdict(asset) for asset in self.assets.values()],
                "dependencies": [asdict(dep) for dep in self.dependencies]
            }
            return json.dumps(graph_data, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
