from typing import List, Dict, Optional
from dataclasses import dataclass
from ..core.lineage_graph import LineageGraph, LineageNode, LineageEdge

@dataclass
class ColumnNode:
    name: str
    dataframe: str
    dependencies: List['ColumnNode'] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

class LineageExtractor:
    def __init__(self):
        self.column_lineage: Dict[str, ColumnNode] = {}

    def extract_from_plan(self, logical_plan: str) -> LineageGraph:
        graph = LineageGraph()
        lines = logical_plan.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if '->' in line:
                parts = line.split('->')
                source = parts[0].strip()
                target = parts[1].strip()
                
                source_node = LineageNode(id=source, type='column')
                target_node = LineageNode(id=target, type='column')
                graph.add_node(source_node)
                graph.add_node(target_node)
                
                edge = LineageEdge(source=source, target=target, type='column_dependency')
                graph.add_edge(edge)
        
        return graph

    def build_lineage(self, df_name: str, columns: List[str], 
                     dependencies: Optional[Dict[str, List[str]]] = None) -> LineageGraph:
        graph = LineageGraph()
        
        for col in columns:
            col_id = f"{df_name}.{col}"
            node = LineageNode(id=col_id, type='column', 
                             metadata={'dataframe': df_name, 'column': col})
            graph.add_node(node)
            
            col_node = ColumnNode(name=col, dataframe=df_name)
            self.column_lineage[col_id] = col_node
            
            if dependencies and col in dependencies:
                for dep in dependencies[col]:
                    dep_node = LineageNode(id=dep, type='column')
                    graph.add_node(dep_node)
                    edge = LineageEdge(source=dep, target=col_id, type='column_dependency')
                    graph.add_edge(edge)
                    
                    dep_col_node = ColumnNode(name=dep.split('.')[-1], 
                                            dataframe=dep.split('.')[0])
                    col_node.dependencies.append(dep_col_node)
        
        return graph
