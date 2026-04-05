import duckdb
from pathlib import Path
from typing import List, Optional
from .models import Term

class GlossaryManager:
    def __init__(self, db_path: str = '.tracepipe/glossary.db'):
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS glossary_terms (
                name VARCHAR PRIMARY KEY,
                definition VARCHAR,
                owner VARCHAR,
                tags VARCHAR,
                quality_score DOUBLE,
                is_pii BOOLEAN,
                steward VARCHAR,
                created_at VARCHAR,
                updated_at VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS catalog_enrichments (
                catalog_name VARCHAR,
                schema_name VARCHAR,
                table_name VARCHAR,
                column_name VARCHAR,
                term_name VARCHAR,
                PRIMARY KEY (catalog_name, schema_name, table_name, column_name)
            )
        """)
        conn.close()
    
    def add_term(self, term: Term) -> None:
        conn = duckdb.connect(self.db_path)
        data = term.to_dict()
        conn.execute("""
            INSERT OR REPLACE INTO glossary_terms 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, list(data.values()))
        conn.close()
    
    def get_term(self, name: str) -> Optional[Term]:
        conn = duckdb.connect(self.db_path)
        result = conn.execute(
            "SELECT * FROM glossary_terms WHERE name = ?", [name]
        ).fetchone()
        conn.close()
        if result:
            cols = ['name', 'definition', 'owner', 'tags', 'quality_score',
                    'is_pii', 'steward', 'created_at', 'updated_at']
            return Term.from_dict(dict(zip(cols, result)))
        return None
    
    def list_terms(self, tag: Optional[str] = None) -> List[Term]:
        conn = duckdb.connect(self.db_path)
        if tag:
            query = "SELECT * FROM glossary_terms WHERE tags LIKE ?"
            results = conn.execute(query, [f'%{tag}%']).fetchall()
        else:
            results = conn.execute("SELECT * FROM glossary_terms").fetchall()
        conn.close()
        cols = ['name', 'definition', 'owner', 'tags', 'quality_score',
                'is_pii', 'steward', 'created_at', 'updated_at']
        return [Term.from_dict(dict(zip(cols, r))) for r in results]