import duckdb
import json
from datetime import datetime
from typing import List, Dict, Optional
from databricks import sql as databricks_sql
import yaml


class LineageArchiver:
    def __init__(self, config_path: str = "config/config.yml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.db_path = self.config.get('lineage_archive_db', 'lineage_archive.duckdb')
        self.conn = duckdb.connect(self.db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                extracted_at TIMESTAMP,
                source_system VARCHAR,
                metadata JSON
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_edges (
                edge_id VARCHAR PRIMARY KEY,
                snapshot_id VARCHAR,
                source_entity VARCHAR,
                target_entity VARCHAR,
                entity_type VARCHAR,
                lineage_type VARCHAR,
                captured_at TIMESTAMP,
                metadata JSON,
                FOREIGN KEY (snapshot_id) REFERENCES lineage_snapshots(snapshot_id)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_edges_source ON lineage_edges(source_entity)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_edges_target ON lineage_edges(target_entity)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_edges_timestamp ON lineage_edges(captured_at)
        """)

    def extract_databricks_lineage(self) -> List[Dict]:
        db_config = self.config.get('databricks', {})
        conn = databricks_sql.connect(
            server_hostname=db_config['server_hostname'],
            http_path=db_config['http_path'],
            access_token=db_config['access_token']
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_catalog, table_schema, table_name, 
                   upstream_tables, downstream_tables, last_updated
            FROM system.information_schema.table_lineage
        """)
        results = cursor.fetchall()
        conn.close()
        return [{
            'catalog': r[0], 'schema': r[1], 'table': r[2],
            'upstream': r[3], 'downstream': r[4], 'updated': r[5]
        } for r in results]

    def archive_lineage(self, lineage_data: Optional[List[Dict]] = None):
        if lineage_data is None:
            lineage_data = self.extract_databricks_lineage()
        
        snapshot_id = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.conn.execute(
            "INSERT INTO lineage_snapshots VALUES (?, ?, ?, ?)",
            (snapshot_id, datetime.now(), 'databricks', json.dumps({}))
        )
        
        for item in lineage_data:
            entity_name = f"{item['catalog']}.{item['schema']}.{item['table']}"
            if item.get('upstream'):
                for upstream in item['upstream']:
                    edge_id = f"{snapshot_id}_{upstream}_{entity_name}"
                    self.conn.execute(
                        "INSERT INTO lineage_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (edge_id, snapshot_id, upstream, entity_name, 'table',
                         'upstream', datetime.now(), json.dumps(item))
                    )
        self.conn.commit()
        return snapshot_id
