class LineageEnricher:
    """Enrich lineage nodes with business context metadata."""
    
    def __init__(self, metadata_store):
        self.metadata_store = metadata_store
        self.enriched_nodes = {}
    
    def enrich_node(self, node_id, term_ids=None, owner_id=None):
        """Associate metadata with a lineage node."""
        if node_id not in self.enriched_nodes:
            self.enriched_nodes[node_id] = {
                'terms': [],
                'owner': None,
                'quality_rules': []
            }
        
        if term_ids:
            for term_id in term_ids:
                term = self.metadata_store.get_term(term_id)
                if term:
                    self.enriched_nodes[node_id]['terms'].append({
                        'term_id': term_id,
                        'term': term
                    })
        
        if owner_id:
            owner = self.metadata_store.get_owner(owner_id)
            if owner:
                self.enriched_nodes[node_id]['owner'] = owner
        
        rules = self.metadata_store.get_quality_rules(node_id)
        if rules:
            self.enriched_nodes[node_id]['quality_rules'] = rules
        
        return self.enriched_nodes[node_id]
    
    def get_enriched_node(self, node_id):
        """Get enriched metadata for a node."""
        return self.enriched_nodes.get(node_id)
    
    def get_all_enriched_nodes(self):
        """Get all enriched nodes."""
        return self.enriched_nodes
