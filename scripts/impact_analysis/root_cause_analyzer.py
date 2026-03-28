"""
Root Cause Analyzer

Analyzes upstream dependencies to identify root causes of data quality issues.
"""

import networkx as nx
from typing import Dict, List, Set
from dataclasses import dataclass


@dataclass
class RootCauseResult:
    """Result of root cause analysis"""
    target_asset: str
    source_tables: List[str]
    source_columns: List[str]
    dependency_depth: int
    total_dependencies: int
    dependency_paths: List[List[str]]
    critical_dependencies: List[str]


class RootCauseAnalyzer:
    """Analyzes upstream dependencies for root cause identification"""
    
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
    
    def analyze_root_cause(self, asset_identifier: str, max_depth: int = 10) -> RootCauseResult:
        """Analyze upstream dependencies to find potential root causes"""
        # Find the asset in the graph
        asset_node = self._find_asset(asset_identifier)
        if not asset_node:
            raise ValueError(f"Asset not found: {asset_identifier}")
        
        # Get all upstream nodes
        upstream_nodes = self._get_upstream_nodes(asset_node, max_depth)
        
        # Categorize upstream assets
        source_tables = []
        source_columns = []
        
        for node in upstream_nodes:
            node_type = self.graph.nodes[node].get('type')
            if node_type == 'table':
                source_tables.append(node)
            elif node_type == 'column':
                source_columns.append(node)
        
        # Find dependency paths
        dependency_paths = self._find_dependency_paths(upstream_nodes, asset_node, max_depth)
        
        # Identify critical dependencies (direct sources)
        critical_deps = self._find_critical_dependencies(asset_node)
        
        # Calculate maximum depth
        max_path_depth = max([len(path) for path in dependency_paths]) if dependency_paths else 0
        
        return RootCauseResult(
            target_asset=asset_node,
            source_tables=source_tables,
            source_columns=source_columns,
            dependency_depth=max_path_depth,
            total_dependencies=len(upstream_nodes),
            dependency_paths=dependency_paths,
            critical_dependencies=critical_deps
        )
    
    def find_common_root_causes(self, asset_identifiers: List[str]) -> Dict:
        """Find common root causes for multiple assets (e.g., multiple failing tables)"""
        all_roots = []
        
        for asset_id in asset_identifiers:
            result = self.analyze_root_cause(asset_id)
            all_roots.append(set(result.source_tables + result.source_columns))
        
        if not all_roots:
            return {'common_roots': [], 'analysis': 'No assets analyzed'}
        
        # Find intersection of all root causes
        common_roots = set.intersection(*all_roots) if len(all_roots) > 1 else all_roots[0]
        
        return {
            'common_roots': list(common_roots),
            'asset_count': len(asset_identifiers),
            'analysis': f"Found {len(common_roots)} common root dependencies",
            'likelihood': 'HIGH' if common_roots else 'LOW'
        }
    
    def diagnose_data_quality_issue(self, asset_identifier: str) -> Dict:
        """Diagnose potential data quality issues by analyzing upstream dependencies"""
        result = self.analyze_root_cause(asset_identifier)
        
        diagnosis = {
            'target_asset': result.target_asset,
            'total_upstream_dependencies': result.total_dependencies,
            'dependency_depth': result.dependency_depth,
            'critical_sources': result.critical_dependencies,
            'recommendations': self._generate_diagnosis_recommendations(result),
            'investigation_priority': self._prioritize_sources(result)
        }
        
        return diagnosis
    
    def _find_asset(self, identifier: str) -> str:
        """Find asset in graph"""
        if identifier in self.graph:
            return identifier
        
        for node in self.graph.nodes():
            if node.endswith(f".{identifier}") or identifier in node:
                return node
        
        return None
    
    def _get_upstream_nodes(self, target_node: str, max_depth: int) -> Set[str]:
        """Get all upstream nodes up to max_depth"""
        upstream = set()
        
        try:
            # Use BFS to find all nodes that can reach the target
            for source in self.graph.nodes():
                if source != target_node:
                    try:
                        path = nx.shortest_path(self.graph, source, target_node)
                        if len(path) <= max_depth + 1:
                            upstream.add(source)
                    except nx.NetworkXNoPath:
                        continue
        except Exception:
            pass
        
        return upstream
    
    def _find_dependency_paths(self, sources: Set[str], target: str, max_depth: int) -> List[List[str]]:
        """Find paths from source nodes to target"""
        paths = []
        
        for source in sources:
            try:
                path = nx.shortest_path(self.graph, source, target)
                if len(path) <= max_depth + 1:
                    paths.append(path)
            except nx.NetworkXNoPath:
                continue
        
        # Return top 10 most important paths (shortest first)
        paths.sort(key=len)
        return paths[:10]
    
    def _find_critical_dependencies(self, target_node: str) -> List[str]:
        """Find direct upstream dependencies (critical sources)"""
        critical = []
        
        # Get direct predecessors
        if target_node in self.graph:
            predecessors = list(self.graph.predecessors(target_node))
            # Filter to only tables and views (not columns)
            for pred in predecessors:
                node_type = self.graph.nodes[pred].get('type')
                if node_type in ['table', 'view']:
                    critical.append(pred)
        
        return critical
    
    def _generate_diagnosis_recommendations(self, result: RootCauseResult) -> List[str]:
        """Generate recommendations for root cause investigation"""
        recommendations = []
        
        if result.total_dependencies == 0:
            recommendations.append("No upstream dependencies found. Issue likely in the asset itself.")
            return recommendations
        
        if result.critical_dependencies:
            recommendations.append(
                f"Start investigation with {len(result.critical_dependencies)} direct source(s): " +
                f"{', '.join(result.critical_dependencies[:3])}"
            )
        
        if result.dependency_depth > 5:
            recommendations.append(
                "Deep dependency chain detected. Issue may originate from early-stage sources."
            )
        
        if result.source_tables:
            recommendations.append(
                f"Check data quality in {len(result.source_tables)} source tables."
            )
        
        recommendations.append("Review transformation logic in intermediate views/tables.")
        recommendations.append("Validate data freshness and completeness of source data.")
        
        return recommendations
    
    def _prioritize_sources(self, result: RootCauseResult) -> List[Dict[str, str]]:
        """Prioritize sources for investigation"""
        priority_list = []
        
        # Critical dependencies get highest priority
        for dep in result.critical_dependencies[:5]:
            priority_list.append({
                'asset': dep,
                'priority': 'HIGH',
                'reason': 'Direct dependency'
            })
        
        # Add other source tables with medium priority
        for table in result.source_tables[:5]:
            if table not in result.critical_dependencies:
                priority_list.append({
                    'asset': table,
                    'priority': 'MEDIUM',
                    'reason': 'Upstream source table'
                })
        
        return priority_list
