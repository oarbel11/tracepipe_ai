class MetadataStore:
    """Store for business glossary terms, owners, and quality rules."""
    
    def __init__(self):
        self.terms = {}
        self.owners = {}
        self.quality_rules = {}
    
    def add_term(self, term_id, name, definition, category=None):
        """Add a business glossary term."""
        self.terms[term_id] = {
            'name': name,
            'definition': definition,
            'category': category
        }
        return term_id
    
    def get_term(self, term_id):
        """Retrieve a business glossary term."""
        return self.terms.get(term_id)
    
    def add_owner(self, entity_id, owner_name, owner_email):
        """Add data owner information."""
        self.owners[entity_id] = {
            'name': owner_name,
            'email': owner_email
        }
        return entity_id
    
    def get_owner(self, entity_id):
        """Retrieve owner information."""
        return self.owners.get(entity_id)
    
    def add_quality_rule(self, rule_id, entity_id, rule_type, rule_def):
        """Add a data quality rule."""
        if entity_id not in self.quality_rules:
            self.quality_rules[entity_id] = []
        rule = {
            'rule_id': rule_id,
            'type': rule_type,
            'definition': rule_def
        }
        self.quality_rules[entity_id].append(rule)
        return rule_id
    
    def get_quality_rules(self, entity_id):
        """Retrieve quality rules for an entity."""
        return self.quality_rules.get(entity_id, [])
    
    def list_terms(self):
        """List all business terms."""
        return list(self.terms.keys())
    
    def list_owners(self):
        """List all owners."""
        return list(self.owners.keys())
