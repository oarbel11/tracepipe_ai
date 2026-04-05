import json
from datetime import datetime, timedelta
import duckdb
from pathlib import Path
from typing import Dict, List, Optional, Any


class LineageArchive:
    def __init__(self, db_path: str = "lineage_archive.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_schema()

    def _initialize_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_metadata (
                id VARCHAR PRIMARY KEY,
                entity_type VARCHAR,
                entity_name VARCHAR,
                upstream_entities JSON,
                downstream_entities JSON,
                metadata JSON,
                captured_at TIMESTAMP,
                source VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_entity_name 
            ON lineage_metadata(entity_name)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_captured_at 
            ON lineage_metadata(captured_at)
        """)

    def archive_lineage(self, lineage_data: Dict[str, Any]) -> bool:
        try:
            entity_id = lineage_data.get("id", f"{lineage_data['entity_name']}_{datetime.now().isoformat()}")
            self.conn.execute("""
                INSERT OR REPLACE INTO lineage_metadata 
                (id, entity_type, entity_name, upstream_entities, 
                 downstream_entities, metadata, captured_at, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                entity_id,
                lineage_data.get("entity_type"),
                lineage_data.get("entity_name"),
                json.dumps(lineage_data.get("upstream_entities", [])),
                json.dumps(lineage_data.get("downstream_entities", [])),
                json.dumps(lineage_data.get("metadata", {})),
                lineage_data.get("captured_at", datetime.now()),
                lineage_data.get("source", "databricks")
            ])
            return True
        except Exception as e:
            print(f"Error archiving lineage: {e}")
            return False

    def query_historical_lineage(self, entity_name: str, 
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> List[Dict]:
        query = "SELECT * FROM lineage_metadata WHERE entity_name = ?"
        params = [entity_name]
        
        if start_date:
            query += " AND captured_at >= ?"
            params.append(start_date)
        if end_date:
            query += " AND captured_at <= ?"
            params.append(end_date)
        
        query += " ORDER BY captured_at DESC"
        result = self.conn.execute(query, params).fetchall()
        return [dict(zip(["id", "entity_type", "entity_name", "upstream_entities",
                          "downstream_entities", "metadata", "captured_at", "source"], row)) 
                for row in result]

    def close(self):
        if self.conn:
            self.conn.close()
