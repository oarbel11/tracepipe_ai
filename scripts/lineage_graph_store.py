import duckdb
from typing import Dict, List, Optional, Tuple
import json

class LineageGraphStore:
    def __init__(self, db_path: str = 'lineage.duckdb'):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id VARCHAR PRIMARY KEY,
                node_type VARCHAR,
                tags VARCHAR[],
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                source_id VARCHAR,
                target_id VARCHAR,
                edge_type VARCHAR,
                metadata JSON,
                PRIMARY KEY (source_id, target_id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tags ON nodes USING GIN(tags)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON edges(source_id)")

    def add_node(self, node_id: str, node_type: str, tags: List[str], metadata: Dict):
        self.conn.execute(
            "INSERT OR REPLACE INTO nodes VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
            [node_id, node_type, tags, json.dumps(metadata)]
        )

    def add_edge(self, source: str, target: str, edge_type: str, metadata: Dict = None):
        self.conn.execute(
            "INSERT OR REPLACE INTO edges VALUES (?, ?, ?, ?)",
            [source, target, edge_type, json.dumps(metadata or {})]
        )

    def find_nodes_by_tags(self, tags: List[str]) -> List[Dict]:
        query = "SELECT * FROM nodes WHERE list_has_any(tags, ?)"
        result = self.conn.execute(query, [tags]).fetchall()
        return [self._row_to_dict(r) for r in result]

    def get_downstream(self, node_id: str, max_depth: int = 3) -> List[str]:
        query = f"""
            WITH RECURSIVE downstream AS (
                SELECT target_id, 1 as depth FROM edges WHERE source_id = ?
                UNION ALL
                SELECT e.target_id, d.depth + 1
                FROM edges e JOIN downstream d ON e.source_id = d.target_id
                WHERE d.depth < {max_depth}
            )
            SELECT DISTINCT target_id FROM downstream
        """
        return [r[0] for r in self.conn.execute(query, [node_id]).fetchall()]

    def _row_to_dict(self, row: Tuple) -> Dict:
        return {
            'node_id': row[0],
            'node_type': row[1],
            'tags': row[2],
            'metadata': json.loads(row[3]) if row[3] else {}
        }

    def close(self):
        self.conn.close()
