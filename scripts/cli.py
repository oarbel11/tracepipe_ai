"""CLI entry point for Tracepipe AI scripts."""
import click
from scripts.databricks_lineage.cli import lineage_command


@click.group()
def cli():
    """Tracepipe AI CLI."""
    pass


cli.add_command(lineage_command)


if __name__ == "__main__":
    cli()
