import click
import json
from scripts.bi_integration import BIIntegrationEngine

@click.group()
def bi_integration():
    """BI Tool Integration commands"""
    pass

@bi_integration.command()
@click.option('--bi-tool', required=True, type=click.Choice(['powerbi', 'tableau', 'looker']))
@click.option('--config', type=str, default='{}')
def sync(bi_tool, config):
    """Sync BI metadata to Unity Catalog"""
    config_dict = json.loads(config) if config else {}
    lineage_data = {}
    
    engine = BIIntegrationEngine(bi_tool, config_dict, lineage_data)
    results = engine.sync_metadata()
    
    click.echo(f"Synced {results['total_metrics']} metrics from {len(results['dashboards'])} dashboards")
    click.echo(f"Created {results['total_mappings']} table mappings")
    click.echo(json.dumps(results, indent=2))

@bi_integration.command()
@click.option('--bi-tool', required=True, type=click.Choice(['powerbi', 'tableau', 'looker']))
@click.option('--metric-name', required=True)
def query_lineage(bi_tool, metric_name):
    """Query lineage for a specific metric"""
    engine = BIIntegrationEngine(bi_tool, {}, {})
    lineage = engine.query_metric_lineage(metric_name)
    
    click.echo(json.dumps(lineage, indent=2))

if __name__ == '__main__':
    bi_integration()
