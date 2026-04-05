"""Track and manage column-level lineage metadata."""
import json
from typing import Dict, List, Optional
from pathlib import Path
from scripts.lineage.lineage_parser import ColumnLineage


class LineageTracker:
    """Manage column lineage metadata with manual overrides."""

    def __init__(self, storage_path: str = "data/lineage"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.lineage_store: Dict[str, List[Dict]] = {}

    def add_lineage(self, table_name: str, lineages: List[ColumnLineage]):
        """Add column lineage for a table."""
        if table_name not in self.lineage_store:
            self.lineage_store[table_name] = []

        for lineage in lineages:
            self.lineage_store[table_name].append({
                'target_column': lineage.target_column,
                'source_columns': lineage.source_columns,
                'transformation': lineage.transformation,
                'source_table': lineage.source_table
            })

    def add_manual_lineage(self, table_name: str, target_column: str,
                          source_columns: List[str], transformation: str = ""):
        """Manually define or override lineage."""
        if table_name not in self.lineage_store:
            self.lineage_store[table_name] = []

        existing = [l for l in self.lineage_store[table_name]
                   if l['target_column'] == target_column]
        if existing:
            existing[0].update({
                'source_columns': source_columns,
                'transformation': transformation,
                'manual_override': True
            })
        else:
            self.lineage_store[table_name].append({
                'target_column': target_column,
                'source_columns': source_columns,
                'transformation': transformation,
                'manual_override': True
            })

    def get_lineage(self, table_name: str) -> List[Dict]:
        """Get lineage for a table."""
        return self.lineage_store.get(table_name, [])

    def save(self):
        """Persist lineage to disk."""
        output_file = self.storage_path / "lineage_metadata.json"
        with open(output_file, 'w') as f:
            json.dump(self.lineage_store, f, indent=2)

    def load(self):
        """Load lineage from disk."""
        input_file = self.storage_path / "lineage_metadata.json"
        if input_file.exists():
            with open(input_file, 'r') as f:
                self.lineage_store = json.load(f)
