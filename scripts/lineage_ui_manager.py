import duckdb
import networkx as nx
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime

class LineageUIManager:
    def __init__(self, db_path: str = "lineage_store.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()
        self.graph = nx.DiGraph()
        self._load_graph()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_edges (
                id INTEGER PRIMARY KEY,
                source_asset VARCHAR,
                target_asset VARCHAR,
                transform_logic TEXT,
                created_at TIMESTAMP,
                is_active BOOLEAN DEFAULT true
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_metadata (
                asset_id VARCHAR PRIMARY KEY,
                classifications TEXT,
                business_terms TEXT,
                tags TEXT,
                masking_policy VARCHAR,
                last_updated TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS lineage_id_seq START 1
        """)

    def _load_graph(self):
        edges = self.conn.execute(
            "SELECT source_asset, target_asset FROM lineage_edges WHERE is_active"
        ).fetchall()
        for source, target in edges:
            self.graph.add_edge(source, target)

    def add_lineage_edge(self, source: str, target: str, transform: str = ""):
        self.conn.execute("""
            INSERT INTO lineage_edges (id, source_asset, target_asset, transform_logic, created_at)
            VALUES (nextval('lineage_id_seq'), ?, ?, ?, ?)
        """, [source, target, transform, datetime.now()])
        self.graph.add_edge(source, target, transform=transform)

    def apply_classification(self, asset_id: str, classification: str, reason: str = ""):
        existing = self.conn.execute(
            "SELECT classifications FROM asset_metadata WHERE asset_id = ?", [asset_id]
        ).fetchone()
        if existing:
            classifications = json.loads(existing[0]) if existing[0] else []
            classifications.append({"type": classification, "reason": reason})
            self.conn.execute(
                "UPDATE asset_metadata SET classifications = ?, last_updated = ? WHERE asset_id = ?",
                [json.dumps(classifications), datetime.now(), asset_id]
            )
        else:
            self.conn.execute("""
                INSERT INTO asset_metadata (asset_id, classifications, last_updated)
                VALUES (?, ?, ?)
            """, [asset_id, json.dumps([{"type": classification, "reason": reason}]), datetime.now()])

    def add_business_term(self, asset_id: str, term: str):
        existing = self.conn.execute(
            "SELECT business_terms FROM asset_metadata WHERE asset_id = ?", [asset_id]
        ).fetchone()
        terms = json.loads(existing[0]) if existing and existing[0] else []
        terms.append(term)
        if existing:
            self.conn.execute(
                "UPDATE asset_metadata SET business_terms = ? WHERE asset_id = ?",
                [json.dumps(terms), asset_id]
            )
        else:
            self.conn.execute(
                "INSERT INTO asset_metadata (asset_id, business_terms) VALUES (?, ?)",
                [asset_id, json.dumps(terms)]
            )

    def analyze_schema_change_impact(self, asset_id: str, changes: List[str]) -> Dict:
        downstream = list(nx.descendants(self.graph, asset_id)) if asset_id in self.graph else []
        upstream = list(nx.ancestors(self.graph, asset_id)) if asset_id in self.graph else []
        return {
            "affected_assets": downstream,
            "dependent_count": len(downstream),
            "upstream_assets": upstream,
            "changes": changes,
            "risk_level": "high" if len(downstream) > 5 else "medium" if len(downstream) > 0 else "low"
        }

    def export_visualization_data(self) -> Dict:
        nodes = []
        edges = []
        for node in self.graph.nodes():
            metadata = self.conn.execute(
                "SELECT classifications, business_terms, tags FROM asset_metadata WHERE asset_id = ?",
                [node]
            ).fetchone()
            nodes.append({
                "id": node,
                "classifications": json.loads(metadata[0]) if metadata and metadata[0] else [],
                "business_terms": json.loads(metadata[1]) if metadata and metadata[1] else [],
                "tags": json.loads(metadata[2]) if metadata and metadata[2] else []
            })
        for source, target in self.graph.edges():
            edges.append({"source": source, "target": target})
        return {"nodes": nodes, "edges": edges}
