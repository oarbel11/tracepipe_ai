import json
import os
from pathlib import Path
import networkx as nx
from connectors import BaseLineageConnector, LineageNode, LineageEdge, ConnectorRegistry

class DbtConnector(BaseLineageConnector):
    def __init__(self, config):
        super().__init__(config)
        self.manifest_path = config.get('manifest_path', 'target/manifest.json')
        self.project_name = config.get('project_name', 'dbt_project')

    def validate_config(self) -> bool:
        return os.path.exists(self.manifest_path)

    def extract_lineage(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        
        with open(self.manifest_path, 'r') as f:
            manifest = json.load(f)
        
        nodes = manifest.get('nodes', {})
        sources = manifest.get('sources', {})
        
        for node_id, node_data in {**nodes, **sources}.items():
            node_type = node_data.get('resource_type', 'unknown')
            unique_id = f"dbt://{self.project_name}/{node_id}"
            
            lineage_node = LineageNode(
                identifier=unique_id,
                node_type=node_type,
                system='dbt',
                metadata={
                    'name': node_data.get('name'),
                    'schema': node_data.get('schema'),
                    'database': node_data.get('database'),
                    'description': node_data.get('description', '')
                }
            )
            
            graph.add_node(unique_id, **lineage_node.to_dict())
            
            for dep in node_data.get('depends_on', {}).get('nodes', []):
                dep_id = f"dbt://{self.project_name}/{dep}"
                edge = LineageEdge(source=dep_id, target=unique_id, edge_type='transforms')
                graph.add_edge(dep_id, unique_id, edge_type=edge.edge_type, metadata=edge.metadata)
        
        return graph

ConnectorRegistry.register('dbt', DbtConnector)
