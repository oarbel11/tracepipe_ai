import duckdb
from typing import Dict, List, Optional
from .manager import GlossaryManager
from .models import Term

class CatalogEnricher:
    def __init__(self, glossary_mgr: GlossaryManager):
        self.glossary = glossary_mgr
    
    def enrich_column(self, catalog: str, schema: str, table: str,
                      column: str, term_name: str) -> None:
        conn = duckdb.connect(self.glossary.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO catalog_enrichments
            VALUES (?, ?, ?, ?, ?)
        """, [catalog, schema, table, column, term_name])
        conn.close()
    
    def get_enrichment(self, catalog: str, schema: str, table: str,
                       column: str) -> Optional[Term]:
        conn = duckdb.connect(self.glossary.db_path)
        result = conn.execute("""
            SELECT term_name FROM catalog_enrichments
            WHERE catalog_name = ? AND schema_name = ?
            AND table_name = ? AND column_name = ?
        """, [catalog, schema, table, column]).fetchone()
        conn.close()
        if result:
            return self.glossary.get_term(result[0])
        return None
    
    def get_table_enrichments(self, catalog: str, schema: str,
                              table: str) -> Dict[str, Term]:
        conn = duckdb.connect(self.glossary.db_path)
        results = conn.execute("""
            SELECT column_name, term_name FROM catalog_enrichments
            WHERE catalog_name = ? AND schema_name = ? AND table_name = ?
        """, [catalog, schema, table]).fetchall()
        conn.close()
        enrichments = {}
        for col, term_name in results:
            term = self.glossary.get_term(term_name)
            if term:
                enrichments[col] = term
        return enrichments
    
    def find_pii_columns(self, catalog: str, schema: str) -> List[Dict]:
        conn = duckdb.connect(self.glossary.db_path)
        results = conn.execute("""
            SELECT e.catalog_name, e.schema_name, e.table_name,
                   e.column_name, t.definition
            FROM catalog_enrichments e
            JOIN glossary_terms t ON e.term_name = t.name
            WHERE e.catalog_name = ? AND e.schema_name = ?
            AND t.is_pii = true
        """, [catalog, schema]).fetchall()
        conn.close()
        return [{'catalog': r[0], 'schema': r[1], 'table': r[2],
                 'column': r[3], 'definition': r[4]} for r in results]