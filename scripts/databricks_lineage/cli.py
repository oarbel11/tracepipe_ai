"""CLI commands for Databricks lineage extraction."""
import json
import click
from .lineage_extractor import DatabricksLineageExtractor
from .lineage_graph import LineageGraphBuilder


@click.command("databricks-lineage")
@click.option("--host", help="Databricks workspace host")
@click.option("--token", help="Databricks access token")
@click.option(
    "--output",
    default="lineage.graphml",
    help="Output file path",
)
@click.option(
    "--format",
    type=click.Choice(["graphml", "json"]),
    default="graphml",
    help="Output format",
)
def lineage_command(host, token, output, format):
    """Extract and visualize Databricks pipeline lineage."""
    click.echo("Extracting Databricks lineage...")

    extractor = DatabricksLineageExtractor(host=host, token=token)

    click.echo("Extracting jobs...")
    jobs = extractor.extract_jobs_lineage()
    click.echo(f"Found {len(jobs)} jobs")

    click.echo("Extracting notebooks...")
    notebooks = extractor.extract_notebooks_lineage()
    click.echo(f"Found {len(notebooks)} notebooks")

    click.echo("Extracting tables...")
    tables = extractor.extract_tables_lineage()
    click.echo(f"Found {len(tables)} tables")

    click.echo("Building lineage graph...")
    builder = LineageGraphBuilder()
    graph = builder.build(jobs, notebooks, tables)

    click.echo(f"Graph has {graph.number_of_nodes()} nodes")
    click.echo(f"Graph has {graph.number_of_edges()} edges")

    if format == "graphml":
        builder.export_graphml(output)
        click.echo(f"Lineage exported to {output}")
    elif format == "json":
        graph_data = builder.export_json()
        with open(output, "w") as f:
            json.dump(graph_data, f, indent=2)
        click.echo(f"Lineage exported to {output}")
