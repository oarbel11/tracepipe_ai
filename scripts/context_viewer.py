from typing import Dict, Any, List
import networkx as nx
from scripts.lineage_enricher import LineageEnricher


class ContextViewer:
    def __init__(self, enricher: LineageEnricher):
        self.enricher = enricher

    def display_node_context(self, lineage_graph: nx.DiGraph, 
                            node_id: str) -> str:
        enriched_node = self.enricher.get_enriched_node(lineage_graph, node_id)
        context = enriched_node['business_context']
        
        output = [f"\n{'='*60}"]
        output.append(f"Entity: {node_id}")
        output.append(f"{'='*60}\n")
        
        glossary = context.get('glossary', [])
        if glossary:
            output.append("Business Glossary:")
            for term in glossary:
                output.append(f"  - {term['term']} [{term['category']}]")
                output.append(f"    {term['definition']}")
            output.append("")
        
        owners = context.get('owners', [])
        if owners:
            output.append("Data Owners:")
            for owner in owners:
                output.append(f"  - {owner['name']} ({owner['role']})")
                output.append(f"    Contact: {owner['contact']}")
            output.append("")
        
        quality_rules = context.get('quality_rules', [])
        if quality_rules:
            output.append("Quality Rules:")
            for rule in quality_rules:
                rule_str = f"  - {rule['type']}: {rule['description']}"
                if 'threshold' in rule:
                    rule_str += f" (threshold: {rule['threshold']})"
                output.append(rule_str)
            output.append("")
        
        if not (glossary or owners or quality_rules):
            output.append("No business context available for this entity.\n")
        
        return "\n".join(output)

    def display_lineage_summary(self, lineage_graph: nx.DiGraph) -> str:
        summary = self.enricher.get_context_summary(lineage_graph)
        
        output = [f"\n{'='*60}"]
        output.append("Lineage Context Summary")
        output.append(f"{'='*60}\n")
        output.append(f"Total Nodes: {summary['total_nodes']}")
        output.append(f"Nodes with Context: {summary['nodes_with_context']}")
        output.append(f"Coverage: {summary['coverage_percentage']:.1f}%\n")
        
        if summary['entities_by_owner']:
            output.append("Entities by Owner:")
            for owner, count in summary['entities_by_owner'].items():
                output.append(f"  - {owner}: {count}")
            output.append("")
        
        if summary['terms_by_category']:
            output.append("Terms by Category:")
            for cat, count in summary['terms_by_category'].items():
                output.append(f"  - {cat}: {count}")
        
        return "\n".join(output)
