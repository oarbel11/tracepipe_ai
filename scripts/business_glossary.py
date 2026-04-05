import duckdb
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

class GlossaryManager:
    def __init__(self, db_path: str = "metadata.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS business_glossary (
                term_id VARCHAR PRIMARY KEY,
                term_name VARCHAR NOT NULL,
                definition TEXT,
                owner VARCHAR,
                category VARCHAR,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS glossary_asset_links (
                link_id VARCHAR PRIMARY KEY,
                term_id VARCHAR,
                asset_path VARCHAR NOT NULL,
                asset_type VARCHAR,
                relationship_type VARCHAR DEFAULT 'describes',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (term_id) REFERENCES business_glossary(term_id)
            )
        """)
        conn.close()

    def add_term(self, term_name: str, definition: str, owner: str = None,
                 category: str = None, metadata: Dict = None) -> str:
        term_id = term_name.lower().replace(' ', '_')
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO business_glossary
            (term_id, term_name, definition, owner, category, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [term_id, term_name, definition, owner, category,
               json.dumps(metadata or {})])
        conn.close()
        return term_id

    def link_term_to_asset(self, term_id: str, asset_path: str,
                           asset_type: str = 'table', relationship: str = 'describes'):
        link_id = f"{term_id}::{asset_path}"
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO glossary_asset_links
            (link_id, term_id, asset_path, asset_type, relationship_type)
            VALUES (?, ?, ?, ?, ?)
        """, [link_id, term_id, asset_path, asset_type, relationship])
        conn.close()

    def get_terms_for_asset(self, asset_path: str) -> List[Dict]:
        conn = duckdb.connect(self.db_path)
        result = conn.execute("""
            SELECT g.term_name, g.definition, g.owner, g.category,
                   l.relationship_type
            FROM glossary_asset_links l
            JOIN business_glossary g ON l.term_id = g.term_id
            WHERE l.asset_path = ?
        """, [asset_path]).fetchall()
        conn.close()
        return [{'term': r[0], 'definition': r[1], 'owner': r[2],
                 'category': r[3], 'relationship': r[4]} for r in result]

    def get_assets_for_term(self, term_id: str) -> List[Dict]:
        conn = duckdb.connect(self.db_path)
        result = conn.execute("""
            SELECT asset_path, asset_type, relationship_type
            FROM glossary_asset_links WHERE term_id = ?
        """, [term_id]).fetchall()
        conn.close()
        return [{'asset': r[0], 'type': r[1], 'relationship': r[2]} for r in result]
