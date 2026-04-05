import networkx as nx
from typing import Dict, Optional
import json

class TransformationVisualizer:
    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
    
    def to_json(self) -> str:
        nodes = []
        edges = []
        
        for node in self.graph.nodes(data=True):
            node_id, attrs = node
            nodes.append({
                'id': node_id,
                'label': node_id.split('.')[-1] if '.' in node_id else node_id,
                'type': attrs.get('type', 'source'),
                'table': attrs.get('table', ''),
                'column': attrs.get('column', '')
            })
        
        for edge in self.graph.edges(data=True):
            source, target, attrs = edge
            edges.append({
                'source': source,
                'target': target,
                'operation': attrs.get('operation', 'unknown'),
                'expression': attrs.get('expression', '')
            })
        
        return json.dumps({'nodes': nodes, 'edges': edges}, indent=2)
    
    def to_mermaid(self) -> str:
        lines = ['graph TD']
        
        for node in self.graph.nodes():
            safe_id = node.replace('.', '_').replace('-', '_')
            label = node.split('.')[-1] if '.' in node else node
            lines.append(f"    {safe_id}[{label}]")
        
        for source, target, attrs in self.graph.edges(data=True):
            safe_source = source.replace('.', '_').replace('-', '_')
            safe_target = target.replace('.', '_').replace('-', '_')
            op = attrs.get('operation', '')
            lines.append(f"    {safe_source} -->|{op}| {safe_target}")
        
        return '\n'.join(lines)
    
    def get_transformation_summary(self) -> Dict:
        summary = {
            'total_columns': self.graph.number_of_nodes(),
            'total_transformations': self.graph.number_of_edges(),
            'operations': {},
            'depth': 0
        }
        
        for _, _, attrs in self.graph.edges(data=True):
            op = attrs.get('operation', 'unknown')
            summary['operations'][op] = summary['operations'].get(op, 0) + 1
        
        if self.graph.number_of_nodes() > 0:
            try:
                depths = [nx.dag_longest_path_length(self.graph)]
                summary['depth'] = max(depths) if depths else 0
            except:
                summary['depth'] = 0
        
        return summary
    
    def find_complex_transformations(self, threshold: int = 3) -> list:
        complex_cols = []
        for node in self.graph.nodes():
            predecessors = list(self.graph.predecessors(node))
            if len(predecessors) >= threshold:
                complex_cols.append({
                    'column': node,
                    'depends_on_count': len(predecessors),
                    'sources': predecessors
                })
        return complex_cols
