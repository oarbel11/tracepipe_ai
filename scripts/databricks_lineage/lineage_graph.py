"""Build lineage graph from extracted data."""
import networkx as nx
from typing import Dict, List, Any


class LineageGraphBuilder:
    """Build a directed graph representing data lineage."""

    def __init__(self):
        self.graph = nx.DiGraph()

    def add_jobs(self, jobs: List[Dict[str, Any]]) -> None:
        """Add job nodes and edges to the graph."""
        for job in jobs:
            job_id = job["id"]
            self.graph.add_node(
                job_id,
                type="job",
                name=job.get("name", ""),
                label=f"Job: {job.get('name', job_id)}",
            )
            for task in job.get("tasks", []):
                if "notebook" in task:
                    notebook_path = task["notebook"]
                    self.graph.add_edge(notebook_path, job_id)

    def add_notebooks(self, notebooks: List[Dict[str, Any]]) -> None:
        """Add notebook nodes to the graph."""
        for notebook in notebooks:
            notebook_id = notebook["id"]
            self.graph.add_node(
                notebook_id,
                type="notebook",
                path=notebook.get("path", ""),
                label=f"Notebook: {notebook.get('path', notebook_id)}",
            )

    def add_tables(self, tables: List[Dict[str, Any]]) -> None:
        """Add table nodes to the graph."""
        for table in tables:
            table_id = table["id"]
            self.graph.add_node(
                table_id,
                type="table",
                name=table.get("name", ""),
                catalog=table.get("catalog", ""),
                schema=table.get("schema", ""),
                label=f"Table: {table_id}",
            )

    def build(self, jobs: List, notebooks: List, tables: List) -> nx.DiGraph:
        """Build the complete lineage graph."""
        self.add_notebooks(notebooks)
        self.add_tables(tables)
        self.add_jobs(jobs)
        return self.graph

    def export_graphml(self, output_path: str) -> None:
        """Export graph to GraphML format."""
        nx.write_graphml(self.graph, output_path)

    def export_json(self) -> Dict[str, Any]:
        """Export graph as JSON."""
        return nx.node_link_data(self.graph)
