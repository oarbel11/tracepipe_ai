class ContextViewer:
    """Display enriched metadata in lineage graphs."""
    
    def __init__(self, lineage_enricher):
        self.lineage_enricher = lineage_enricher
    
    def get_node_context(self, node_id):
        """Get formatted context for a specific node."""
        enriched = self.lineage_enricher.get_enriched_node(node_id)
        if not enriched:
            return None
        
        context = {
            'node_id': node_id,
            'business_terms': [],
            'owner': enriched.get('owner'),
            'quality_rules': enriched.get('quality_rules', [])
        }
        
        for term_data in enriched.get('terms', []):
            context['business_terms'].append(term_data['term'])
        
        return context
    
    def get_lineage_with_context(self, lineage_graph):
        """Enrich a lineage graph structure with context."""
        enriched_graph = {'nodes': [], 'edges': lineage_graph.get('edges', [])}
        
        for node in lineage_graph.get('nodes', []):
            node_id = node.get('id')
            enriched_node = dict(node)
            context = self.get_node_context(node_id)
            if context:
                enriched_node['context'] = context
            enriched_graph['nodes'].append(enriched_node)
        
        return enriched_graph
    
    def search_by_term(self, term_name):
        """Find all nodes associated with a business term."""
        results = []
        all_nodes = self.lineage_enricher.get_all_enriched_nodes()
        
        for node_id, enriched in all_nodes.items():
            for term_data in enriched.get('terms', []):
                if term_data['term']['name'] == term_name:
                    results.append(node_id)
                    break
        
        return results
