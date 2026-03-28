"""
Dependency Visualizer

Visualizes lineage graphs and dependency relationships.
"""

import networkx as nx
from typing import List, Optional


class DependencyVisualizer:
    """Visualizes dependency graphs in text format"""
    
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
    
    def visualize_impact(self, source_asset: str, affected_assets: List[str], 
                        impact_paths: List[List[str]]) -> str:
        """Create text visualization of impact analysis"""
        output = []
        output.append(f"\n{'='*80}")
        output.append(f"BLAST RADIUS ANALYSIS: {source_asset}")
        output.append(f"{'='*80}\n")
        
        if not affected_assets:
            output.append("✓ No downstream dependencies found. Change is safe.\n")
            return "\n".join(output)
        
        output.append(f"⚠️  Total affected assets: {len(affected_assets)}\n")
        
        # Show dependency tree for top paths
        output.append("DEPENDENCY PATHS:\n")
        for i, path in enumerate(impact_paths[:5], 1):
            output.append(f"Path {i}:")
            output.append(self._format_path(path))
            output.append("")
        
        if len(impact_paths) > 5:
            output.append(f"... and {len(impact_paths) - 5} more paths\n")
        
        return "\n".join(output)
    
    def visualize_root_cause(self, target_asset: str, source_assets: List[str],
                            dependency_paths: List[List[str]]) -> str:
        """Create text visualization of root cause analysis"""
        output = []
        output.append(f"\n{'='*80}")
        output.append(f"ROOT CAUSE ANALYSIS: {target_asset}")
        output.append(f"{'='*80}\n")
        
        if not source_assets:
            output.append("✓ No upstream dependencies found.\n")
            return "\n".join(output)
        
        output.append(f"📊 Total upstream dependencies: {len(source_assets)}\n")
        
        # Show dependency tree for top paths
        output.append("DEPENDENCY PATHS (from source to target):\n")
        for i, path in enumerate(dependency_paths[:5], 1):
            output.append(f"Path {i}:")
            output.append(self._format_path(path))
            output.append("")
        
        if len(dependency_paths) > 5:
            output.append(f"... and {len(dependency_paths) - 5} more paths\n")
        
        return "\n".join(output)
    
    def visualize_graph_summary(self) -> str:
        """Create summary of the lineage graph"""
        output = []
        output.append(f"\n{'='*80}")
        output.append("DATA LINEAGE GRAPH SUMMARY")
        output.append(f"{'='*80}\n")
        
        # Count nodes by type
        node_types = {}
        for node in self.graph.nodes():
            node_type = self.graph.nodes[node].get('type', 'unknown')
            node_types[node_type] = node_types.get(node_type, 0) + 1
        
        output.append("Node counts:")
        for node_type, count in sorted(node_types.items()):
            output.append(f"  {node_type}: {count}")
        
        output.append(f"\nTotal edges: {self.graph.number_of_edges()}")
        output.append(f"Total nodes: {self.graph.number_of_nodes()}\n")
        
        return "\n".join(output)
    
    def _format_path(self, path: List[str]) -> str:
        """Format a path as a visual tree"""
        if not path:
            return "  (empty path)"
        
        lines = []
        for i, node in enumerate(path):
            indent = "  " * i
            node_type = self.graph.nodes[node].get('type', 'unknown')
            icon = self._get_icon(node_type)
            
            if i == 0:
                lines.append(f"  {icon} {node}")
            else:
                lines.append(f"{indent}└─> {icon} {node}")
        
        return "\n".join(lines)
    
    def _get_icon(self, node_type: str) -> str:
        """Get icon for node type"""
        icons = {
            'table': '📋',
            'view': '👁️',
            'column': '📊',
            'unknown': '❓'
        }
        return icons.get(node_type, '❓')
    
    def export_to_dot(self, output_file: str, subgraph_nodes: Optional[List[str]] = None):
        """Export graph to DOT format for Graphviz visualization"""
        if subgraph_nodes:
            subgraph = self.graph.subgraph(subgraph_nodes)
        else:
            subgraph = self.graph
        
        try:
            nx.drawing.nx_pydot.write_dot(subgraph, output_file)
            return f"Graph exported to {output_file}"
        except Exception as e:
            return f"Could not export graph: {e}"
