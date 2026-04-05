import click
import json
from pathlib import Path
from scripts.impact_simulator import ImpactSimulator
from scripts.graph_visualizer import GraphVisualizer


@click.command()
@click.option('--node', required=True, help='Node ID to analyze')
@click.option('--change-type', default='schema_change',
              help='Type of change (schema_change, deprecation, etc.)')
@click.option('--lineage-file', default='lineage.json',
              help='Path to lineage data file')
@click.option('--format', default='ascii', type=click.Choice(['ascii', 'json', 'mermaid']),
              help='Output format')
def analyze_impact(node, change_type, lineage_file, format):
    """Analyze impact of changes on downstream dependencies."""
    lineage_path = Path(lineage_file)
    if not lineage_path.exists():
        click.echo(f"Error: Lineage file {lineage_file} not found")
        return
    with open(lineage_path, 'r') as f:
        lineage_data = json.load(f)
    simulator = ImpactSimulator(lineage_data)
    impact_result = simulator.simulate_change(node, change_type)
    click.echo(f"\nImpact Analysis for {node}")
    click.echo(f"Change Type: {change_type}")
    click.echo(f"Affected Nodes: {impact_result['affected_count']}")
    click.echo("\nImpact Details:")
    for detail in impact_result['impact_details']:
        click.echo(f"  - {detail['name']} ({detail['type']}) - "
                   f"Severity: {detail['impact_severity']}")
    graph_data = simulator.get_impact_graph(node)
    visualizer = GraphVisualizer(graph_data)
    click.echo("\nDependency Graph:")
    if format == 'ascii':
        click.echo(visualizer.generate_ascii())
    elif format == 'json':
        click.echo(visualizer.generate_json())
    elif format == 'mermaid':
        click.echo(visualizer.generate_mermaid())


if __name__ == '__main__':
    analyze_impact()
