import networkx as nx
from typing import Dict, List
import json
import logging

logger = logging.getLogger(__name__)


class LineageGraphBuilder:
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_table_lineage(self, lineage: List[Dict]):
        """Add table-to-table lineage edges."""
        for edge in lineage:
            source = f"{edge['source_catalog']}.{edge['source_schema']}.{edge['source_table']}"
            target = f"{edge['target_catalog']}.{edge['target_schema']}.{edge['target_table']}"
            self.graph.add_edge(source, target, type='table_lineage')
            self.graph.nodes[source]['asset_type'] = 'table'
            self.graph.nodes[target]['asset_type'] = 'table'

    def add_job_lineage(self, jobs: List[Dict]):
        """Add job nodes and infer table connections."""
        for job in jobs:
            job_node = f"job:{job['job_id']}:{job['name']}"
            self.graph.add_node(job_node, asset_type='job', name=job['name'])
            
            for task in job.get('tasks', []):
                if 'notebook_task' in task:
                    notebook_path = task['notebook_task'].get('notebook_path', '')
                    notebook_node = f"notebook:{notebook_path}"
                    self.graph.add_edge(job_node, notebook_node, type='job_to_notebook')
                    self.graph.nodes[notebook_node]['asset_type'] = 'notebook'

    def add_dlt_lineage(self, pipelines: List[Dict]):
        """Add DLT pipeline nodes."""
        for pipeline in pipelines:
            pipeline_node = f"dlt:{pipeline['pipeline_id']}:{pipeline['name']}"
            self.graph.add_node(pipeline_node, asset_type='dlt_pipeline', 
                              name=pipeline['name'], state=pipeline.get('state', ''))

    def get_upstream(self, asset: str) -> List[str]:
        """Get all upstream dependencies."""
        if asset not in self.graph:
            return []
        return list(self.graph.predecessors(asset))

    def get_downstream(self, asset: str) -> List[str]:
        """Get all downstream dependencies."""
        if asset not in self.graph:
            return []
        return list(self.graph.successors(asset))

    def export_json(self, filepath: str):
        """Export lineage graph to JSON."""
        data = nx.node_link_data(self.graph)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Lineage graph exported to {filepath}")

    def get_stats(self) -> Dict:
        """Return graph statistics."""
        return {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'tables': len([n for n, d in self.graph.nodes(data=True) if d.get('asset_type') == 'table']),
            'jobs': len([n for n, d in self.graph.nodes(data=True) if d.get('asset_type') == 'job']),
            'notebooks': len([n for n, d in self.graph.nodes(data=True) if d.get('asset_type') == 'notebook']),
            'dlt_pipelines': len([n for n, d in self.graph.nodes(data=True) if d.get('asset_type') == 'dlt_pipeline'])
        }
