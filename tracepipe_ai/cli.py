import click
import json
from tracepipe_ai.lineage_history import LineageHistoryStorage


@click.group()
def cli():
    """Tracepipe AI - Data Lineage and Observability Platform"""
    pass


@cli.command()
@click.option('--input-file', required=True, help='JSON file with lineage data')
@click.option('--db-path', default='lineage_history.duckdb',
              help='Path to DuckDB database')
def export_lineage(input_file, db_path):
    """Export lineage data to historical storage."""
    storage = LineageHistoryStorage(db_path)
    
    with open(input_file, 'r') as f:
        lineage_data = json.load(f)
    
    snapshot_id = storage.store_lineage(lineage_data)
    click.echo(f"Lineage exported with snapshot ID: {snapshot_id}")


@cli.command()
@click.option('--start-date', help='Start date (ISO format)')
@click.option('--end-date', help='End date (ISO format)')
@click.option('--table-name', help='Filter by table name')
@click.option('--db-path', default='lineage_history.duckdb',
              help='Path to DuckDB database')
def query_lineage(start_date, end_date, table_name, db_path):
    """Query historical lineage data."""
    storage = LineageHistoryStorage(db_path)
    
    results = storage.query_lineage(
        start_date=start_date,
        end_date=end_date,
        table_name=table_name
    )
    
    click.echo(json.dumps(results, indent=2, default=str))


if __name__ == '__main__':
    cli()
