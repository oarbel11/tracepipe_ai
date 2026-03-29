import json
from typing import Dict, List, Set


class LineageVisualizer:
    def __init__(self, impact_analyzer):
        self.analyzer = impact_analyzer

    def generate_interactive_view(self, column_fqn: str, depth: int = 3) -> Dict:
        impact = self.analyzer.analyze_impact(column_fqn)
        if 'error' in impact:
            return impact
        
        graph_data = self._build_graph_data(column_fqn, depth)
        drill_down_data = self._build_drill_down_data(column_fqn)
        
        return {
            'type': 'interactive_lineage',
            'root_column': column_fqn,
            'graph': graph_data,
            'drill_down': drill_down_data,
            'impact_summary': impact,
            'metadata': self._get_column_metadata(column_fqn)
        }

    def _build_graph_data(self, column: str, depth: int) -> Dict:
        nodes = []
        edges = []
        visited = set()
        
        def traverse(col, current_depth):
            if current_depth > depth or col in visited:
                return
            visited.add(col)
            
            if col in self.analyzer.graph:
                node_data = self.analyzer.graph.nodes[col]
                nodes.append({
                    'id': col,
                    'label': col.split('.')[-1],
                    'table': col.split('.')[0],
                    'type': node_data.get('transformation', {}).get('type', 'unknown'),
                    'depth': current_depth
                })
                
                for successor in self.analyzer.graph.successors(col):
                    edges.append({'source': col, 'target': successor})
                    traverse(successor, current_depth + 1)
        
        traverse(column, 0)
        return {'nodes': nodes, 'edges': edges}

    def _build_drill_down_data(self, column: str) -> List[Dict]:
        drill_down = []
        if column not in self.analyzer.column_metadata:
            return drill_down
        
        metadata = self.analyzer.column_metadata[column]
        transformation = metadata.get('transformation', {})
        
        drill_down.append({
            'level': 'column',
            'name': column,
            'transformation_type': transformation.get('type', 'unknown'),
            'expression': transformation.get('expression', ''),
            'udfs': transformation.get('udfs', [])
        })
        
        for dep in metadata.get('dependencies', []):
            if dep in self.analyzer.column_metadata:
                dep_meta = self.analyzer.column_metadata[dep]
                drill_down.append({
                    'level': 'dependency',
                    'name': dep,
                    'transformation_type': dep_meta.get('transformation', {}).get('type', 'unknown')
                })
        
        return drill_down

    def _get_column_metadata(self, column: str) -> Dict:
        if column not in self.analyzer.graph:
            return {}
        node_data = self.analyzer.graph.nodes[column]
        return {
            'table': node_data.get('table', ''),
            'column': node_data.get('column', ''),
            'transformation': node_data.get('transformation', {})
        }

    def export_json(self, column_fqn: str, filepath: str):
        view_data = self.generate_interactive_view(column_fqn)
        with open(filepath, 'w') as f:
            json.dump(view_data, f, indent=2)

    def export_markdown(self, column_fqn: str) -> str:
        view = self.generate_interactive_view(column_fqn)
        if 'error' in view:
            return f"# Error\n{view['error']}"
        
        md = [f"# Column Lineage: {column_fqn}\n"]
        impact = view['impact_summary']
        md.append(f"**Impact:** {impact['impact_count']} downstream columns")
        md.append(f"**Criticality:** {impact['criticality']}")
        md.append(f"**Risk Score:** {impact['risk_score']:.2f}\n")
        
        md.append("## Transformation Chain\n")
        for item in view['drill_down']:
            md.append(f"- **{item['name']}** ({item['transformation_type']})")
            if item.get('expression'):
                md.append(f"  - Expression: `{item['expression']}`")
        
        return '\n'.join(md)
