"""
╔══════════════════════════════════════════════════════════════════╗
║                     scripts/schema_drift.py                       ║
║              📐 Schema Drift Detector for Pipelines               ║
╠══════════════════════════════════════════════════════════════════╣
║  Detects schema changes (added/removed columns, type changes)     ║
║  and cross-references downstream impact via lineage metadata.     ║
║                                                                   ║
║  Works with DuckDB and Databricks (Unity Catalog).                ║
╚══════════════════════════════════════════════════════════════════╝

USAGE:
    from scripts.schema_drift import SchemaDriftDetector

    detector = SchemaDriftDetector(engine)
    report = detector.detect_drift()
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger('SchemaDrift')


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class DriftChange:
    """A single schema change detected in a table."""
    table: str
    change_type: str          # 'added', 'removed', 'type_changed'
    column_name: str
    old_type: Optional[str]   # None for 'added'
    new_type: Optional[str]   # None for 'removed'
    risk: str                 # 'GREEN', 'YELLOW', 'RED'
    downstream_tables: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def symbol(self) -> str:
        return {'added': '+', 'removed': '-', 'type_changed': '~'}[self.change_type]

    @property
    def risk_emoji(self) -> str:
        return {'GREEN': '🟢', 'YELLOW': '🟡', 'RED': '🔴'}[self.risk]


@dataclass
class DriftReport:
    """Full schema drift report across all tables."""
    changes: List[DriftChange] = field(default_factory=list)
    tables_scanned: int = 0
    tables_with_drift: int = 0
    snapshot_timestamp: str = ""
    previous_snapshot_timestamp: str = ""

    def to_dict(self) -> dict:
        return {
            'tables_scanned': self.tables_scanned,
            'tables_with_drift': self.tables_with_drift,
            'snapshot_timestamp': self.snapshot_timestamp,
            'previous_snapshot_timestamp': self.previous_snapshot_timestamp,
            'total_changes': len(self.changes),
            'risk_summary': {
                'RED': sum(1 for c in self.changes if c.risk == 'RED'),
                'YELLOW': sum(1 for c in self.changes if c.risk == 'YELLOW'),
                'GREEN': sum(1 for c in self.changes if c.risk == 'GREEN'),
            },
            'changes': [c.to_dict() for c in self.changes],
        }

    @property
    def formatted_output(self) -> str:
        """Human-readable drift report."""
        if not self.changes:
            return (
                "\n"
                "╔═══════════════════════════════════════════════════════════════╗\n"
                "║  📐 SCHEMA DRIFT REPORT                                       ║\n"
                "╠═══════════════════════════════════════════════════════════════╣\n"
                f"║  ✅ No schema drift detected across {self.tables_scanned} tables.              ║\n"
                "╚═══════════════════════════════════════════════════════════════╝\n"
            )

        lines = [
            "",
            "╔═══════════════════════════════════════════════════════════════╗",
            "║  📐 SCHEMA DRIFT REPORT                                       ║",
            "╠═══════════════════════════════════════════════════════════════╣",
            f"║  📊 Tables scanned: {self.tables_scanned}",
            f"║  ⚠️  Tables with drift: {self.tables_with_drift}",
            f"║  📅 Previous snapshot: {self.previous_snapshot_timestamp}",
            f"║  📅 Current snapshot:  {self.snapshot_timestamp}",
            "║",
        ]

        # Group changes by table
        by_table: Dict[str, List[DriftChange]] = {}
        for change in self.changes:
            by_table.setdefault(change.table, []).append(change)

        for table, table_changes in by_table.items():
            worst_risk = 'GREEN'
            for c in table_changes:
                if c.risk == 'RED':
                    worst_risk = 'RED'
                elif c.risk == 'YELLOW' and worst_risk != 'RED':
                    worst_risk = 'YELLOW'

            risk_emoji = {'GREEN': '🟢', 'YELLOW': '🟡', 'RED': '🔴'}[worst_risk]
            lines.append(f"║  {risk_emoji} {table}")

            for c in table_changes:
                if c.change_type == 'added':
                    lines.append(f"║     + {c.column_name} ({c.new_type})  ← added")
                elif c.change_type == 'removed':
                    lines.append(f"║     - {c.column_name} ({c.old_type})  ← removed")
                elif c.change_type == 'type_changed':
                    lines.append(f"║     ~ {c.column_name}: {c.old_type} → {c.new_type}  ← type changed")

            # Show downstream impact
            downstream = set()
            for c in table_changes:
                downstream.update(c.downstream_tables)
            if downstream:
                lines.append(f"║     └─ Downstream impact: {', '.join(sorted(downstream))}")
            lines.append("║")

        # Risk summary
        red = sum(1 for c in self.changes if c.risk == 'RED')
        yellow = sum(1 for c in self.changes if c.risk == 'YELLOW')
        green = sum(1 for c in self.changes if c.risk == 'GREEN')
        lines.append(f"║  Risk summary: 🔴 {red}  🟡 {yellow}  🟢 {green}")
        lines.append("║")
        lines.append("╚═══════════════════════════════════════════════════════════════╝")
        lines.append("")

        return '\n'.join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TYPE CHANGE CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Type widening (safe): smaller type → larger type
SAFE_WIDENINGS = {
    ('INTEGER', 'BIGINT'),
    ('INT', 'BIGINT'),
    ('FLOAT', 'DOUBLE'),
    ('REAL', 'DOUBLE'),
    ('SMALLINT', 'INTEGER'),
    ('SMALLINT', 'INT'),
    ('SMALLINT', 'BIGINT'),
    ('TINYINT', 'SMALLINT'),
    ('TINYINT', 'INTEGER'),
    ('TINYINT', 'INT'),
    ('TINYINT', 'BIGINT'),
    ('VARCHAR', 'TEXT'),
    ('STRING', 'TEXT'),
}


def is_safe_widening(old_type: str, new_type: str) -> bool:
    """Check if a type change is a safe widening (no data loss)."""
    return (old_type.upper(), new_type.upper()) in SAFE_WIDENINGS


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCHEMA DRIFT DETECTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SchemaDriftDetector:
    """
    📐 Detects schema drift across all tables in the database.

    Workflow:
        1. snapshot_all()  — capture current schema to disk
        2. detect_drift()  — compare against previous snapshot + blast radius

    Snapshots are stored as JSON in config/schema_snapshots/.
    """

    SNAPSHOT_DIR_NAME = 'schema_snapshots'

    def __init__(self, engine=None, snapshot_dir: Optional[Path] = None):
        """
        Args:
            engine: DebugEngine instance (creates one if not provided)
            snapshot_dir: Custom directory for snapshots (default: config/schema_snapshots/)
        """
        if engine is None:
            from debug_engine import DebugEngine
            engine = DebugEngine()
        self.engine = engine

        if snapshot_dir is None:
            project_root = Path(__file__).parent.parent
            snapshot_dir = project_root / 'config' / self.SNAPSHOT_DIR_NAME
        self.snapshot_dir = snapshot_dir

    # ─────────────────────────────────────────────────────────────
    # SNAPSHOT
    # ─────────────────────────────────────────────────────────────

    def _capture_current_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Query the database and return the schema of every table.

        Returns:
            { "schema.table": [ {"column_name": "x", "column_type": "INTEGER"}, ... ] }
        """
        schema_map: Dict[str, List[Dict[str, str]]] = {}
        tables = self.engine.list_tables()

        for t in tables:
            full_name = f"{t['table_schema']}.{t['table_name']}"
            try:
                columns = self.engine.describe_table(full_name)
                # Normalize to just column_name + column_type
                schema_map[full_name] = [
                    {'column_name': c['column_name'], 'column_type': c['column_type']}
                    for c in columns
                ]
            except Exception as e:
                logger.warning(f"Could not describe {full_name}: {e}")

        return schema_map

    def snapshot_all(self) -> Path:
        """
        Capture the current schema of all tables and save to disk.

        Returns:
            Path to the saved snapshot file.
        """
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        schema = self._capture_current_schema()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        snapshot_file = self.snapshot_dir / f'snapshot_{timestamp}.json'

        payload = {
            'timestamp': timestamp,
            'tables': schema,
        }

        with open(snapshot_file, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)

        logger.info(f"Schema snapshot saved: {snapshot_file} ({len(schema)} tables)")
        return snapshot_file

    def _get_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Load the most recent snapshot from disk."""
        if not self.snapshot_dir.exists():
            return None

        files = sorted(self.snapshot_dir.glob('snapshot_*.json'), reverse=True)
        if not files:
            return None

        with open(files[0], 'r', encoding='utf-8') as f:
            return json.load(f)

    # ─────────────────────────────────────────────────────────────
    # COMPARISON
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def compare(
        previous: Dict[str, List[Dict[str, str]]],
        current: Dict[str, List[Dict[str, str]]],
    ) -> List[DriftChange]:
        """
        Compare two schema snapshots and return a list of changes.

        Args:
            previous: { "table": [{"column_name": ..., "column_type": ...}] }
            current:  same format

        Returns:
            List of DriftChange objects (without downstream info filled in yet).
        """
        changes: List[DriftChange] = []

        all_tables = set(list(previous.keys()) + list(current.keys()))

        for table in sorted(all_tables):
            prev_cols = {c['column_name']: c['column_type'] for c in previous.get(table, [])}
            curr_cols = {c['column_name']: c['column_type'] for c in current.get(table, [])}

            # Columns added
            for col in sorted(set(curr_cols) - set(prev_cols)):
                changes.append(DriftChange(
                    table=table,
                    change_type='added',
                    column_name=col,
                    old_type=None,
                    new_type=curr_cols[col],
                    risk='GREEN',  # Will be upgraded later if downstream impact
                ))

            # Columns removed
            for col in sorted(set(prev_cols) - set(curr_cols)):
                changes.append(DriftChange(
                    table=table,
                    change_type='removed',
                    column_name=col,
                    old_type=prev_cols[col],
                    new_type=None,
                    risk='RED',
                ))

            # Type changes
            for col in sorted(set(prev_cols) & set(curr_cols)):
                if prev_cols[col].upper() != curr_cols[col].upper():
                    safe = is_safe_widening(prev_cols[col], curr_cols[col])
                    changes.append(DriftChange(
                        table=table,
                        change_type='type_changed',
                        column_name=col,
                        old_type=prev_cols[col],
                        new_type=curr_cols[col],
                        risk='YELLOW' if safe else 'RED',
                    ))

        return changes

    # ─────────────────────────────────────────────────────────────
    # DETECT (full workflow)
    # ─────────────────────────────────────────────────────────────

    def detect_drift(self) -> DriftReport:
        """
        Full drift detection workflow:
        1. Load previous snapshot
        2. Capture current schema
        3. Compare
        4. Enrich with downstream impact
        5. Adjust risk levels

        Returns:
            DriftReport with all changes and risk assignments.
        """
        # Load previous snapshot
        prev_data = self._get_latest_snapshot()
        if prev_data is None:
            # No previous snapshot — take one now and return empty report
            logger.info("No previous snapshot found. Taking initial snapshot...")
            self.snapshot_all()
            current_schema = self._capture_current_schema()
            return DriftReport(
                tables_scanned=len(current_schema),
                tables_with_drift=0,
                snapshot_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                previous_snapshot_timestamp='(none — initial snapshot taken)',
            )

        previous_schema = prev_data['tables']
        previous_ts = prev_data.get('timestamp', 'unknown')

        # Capture current schema
        current_schema = self._capture_current_schema()
        current_ts = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Compare
        changes = self.compare(previous_schema, current_schema)

        # Enrich with downstream impact and adjust risk
        tables_with_changes = set(c.table for c in changes)
        for change in changes:
            try:
                downstream = self.engine.get_downstream_tables(change.table)
                change.downstream_tables = downstream
            except Exception:
                change.downstream_tables = []

            # Upgrade risk if downstream impact is significant
            if change.change_type == 'added' and len(change.downstream_tables) > 0:
                change.risk = 'YELLOW'
            if len(change.downstream_tables) > 3 and change.risk != 'RED':
                change.risk = 'YELLOW'
            if change.change_type == 'removed' or (
                change.change_type == 'type_changed' and not is_safe_widening(
                    change.old_type or '', change.new_type or ''
                )
            ):
                change.risk = 'RED'

        # Save new snapshot
        self.snapshot_all()

        return DriftReport(
            changes=changes,
            tables_scanned=len(current_schema),
            tables_with_drift=len(tables_with_changes),
            snapshot_timestamp=current_ts,
            previous_snapshot_timestamp=previous_ts,
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI ENTRY POINT (for standalone use)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'snapshot':
        detector = SchemaDriftDetector()
        path = detector.snapshot_all()
        print(f"\n✅ Snapshot saved to: {path}")
    else:
        detector = SchemaDriftDetector()
        report = detector.detect_drift()
        print(report.formatted_output)
