"""
╔══════════════════════════════════════════════════════════════════╗
║                  scripts/build_metadata.py                        ║
║                 🗺️ GENERIC Metadata Builder                       ║
╠══════════════════════════════════════════════════════════════════╣
║  CLIENT-AGNOSTIC: Parses ANY SQL files!                           ║
║                                                                   ║
║  • No hardcoded table names                                       ║
║  • Works with any schema structure                                ║
║  • Auto-detects SQL file location                                 ║
╚══════════════════════════════════════════════════════════════════╝

USAGE:
    python scripts/build_metadata.py

    # Or with custom paths:
    python scripts/build_metadata.py --sql-dir /path/to/sql --db /path/to/db.duckdb
"""

import re
import sys
import os
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Set, Tuple, Optional
from contextlib import contextmanager

import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('MetadataBuilder')


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class TableLineage:
    """Records a table-to-table relationship."""
    target_table: str
    source_table: str
    sql_text: str


@dataclass
class ColumnLineage:
    """Records column-level transformation logic."""
    target_table: str
    target_column: str
    source_table: str
    source_column: str
    transformation_logic: str
    sql_file_name: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SQL PARSER (Generic - works with any SQL dialect)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class GenericSQLParser:
    """
    Parses SQL files to extract lineage information.

    Works with: DuckDB, Spark SQL, Standard SQL, most dialects.
    """

    # Pattern: CREATE [OR REPLACE] TABLE/VIEW [schema.]table
    CREATE_PATTERN = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )

    # Pattern: FROM/JOIN [schema.]table
    SOURCE_PATTERN = re.compile(
        r"\b(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )

    # Pattern: CASE ... END AS column
    CASE_PATTERN = re.compile(
        r"(CASE\s+.+?END)\s+(?:AS\s+)?(\w+)",
        re.IGNORECASE | re.DOTALL
    )

    # Pattern: aggregate(x) AS column
    AGG_PATTERN = re.compile(
        r"((?:SUM|AVG|COUNT|MIN|MAX|COALESCE|NULLIF)\s*\([^)]+\))\s+(?:AS\s+)?(\w+)",
        re.IGNORECASE
    )

    def clean_sql(self, sql: str) -> str:
        """Remove comments from SQL."""
        lines = []
        for line in sql.split('\n'):
            if '--' in line:
                line = line.split('--')[0]
            lines.append(line)
        sql = '\n'.join(lines)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql

    def parse_statement(self, sql: str, filename: str) -> Tuple[List[TableLineage], List[ColumnLineage]]:
        """Parse a single SQL statement."""
        table_lineages = []
        column_lineages = []

        clean_sql = self.clean_sql(sql)

        create_match = self.CREATE_PATTERN.search(clean_sql)
        if not create_match:
            return [], []

        target_table = create_match.group(1)
        logger.info(f"  ✅ Found: {target_table}")

        # Find source tables
        sources: Set[str] = set()
        for match in self.SOURCE_PATTERN.finditer(clean_sql):
            source = match.group(1)
            if source.lower() != target_table.lower():
                sources.add(source)

        if sources:
            logger.info(f"     └─ Sources: {', '.join(sources)}")

        # Table lineage
        for source in sources:
            table_lineages.append(TableLineage(target_table, source, clean_sql))

        # CASE expressions
        for match in self.CASE_PATTERN.finditer(clean_sql):
            logic, column = match.group(1).strip(), match.group(2).strip()
            logger.info(f"     └─ Computed: {column}")
            column_lineages.append(ColumnLineage(
                target_table, column,
                ', '.join(sources) if sources else 'UNKNOWN',
                'COMPUTED', logic, filename
            ))

        # Aggregations
        for match in self.AGG_PATTERN.finditer(clean_sql):
            logic, column = match.group(1).strip(), match.group(2).strip()
            if any(cl.target_column == column for cl in column_lineages):
                continue
            logger.info(f"     └─ Aggregation: {column}")
            column_lineages.append(ColumnLineage(
                target_table, column,
                ', '.join(sources) if sources else 'UNKNOWN',
                'AGGREGATED', logic, filename
            ))

        return table_lineages, column_lineages

    def parse_file(self, file_path: Path) -> Tuple[List[TableLineage], List[ColumnLineage]]:
        """Parse an entire SQL file."""
        all_table, all_column = [], []
        content = file_path.read_text(encoding='utf-8')

        for statement in content.split(';'):
            statement = statement.strip()
            if statement:
                tl, cl = self.parse_statement(statement, file_path.name)
                all_table.extend(tl)
                all_column.extend(cl)

        return all_table, all_column


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# METADATA BUILDER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MetadataBuilder:
    """Builds metadata tables from SQL files. Generic - works with any database."""

    def __init__(self, db_path: str, sql_dir: Optional[str] = None, meta_schema: str = 'meta'):
        self.db_path = db_path
        self.sql_dir = Path(sql_dir) if sql_dir else None
        self.meta_schema = meta_schema
        self.parser = GenericSQLParser()

    @contextmanager
    def _connect(self, read_only: bool = False):
        con = duckdb.connect(self.db_path, read_only=read_only)
        try:
            yield con
        finally:
            con.close()

    def build(self) -> bool:
        """Build metadata tables. Returns True if successful."""
        print()
        print('=' * 60)
        print('🗺️  METADATA BUILDER (Generic)')
        print('=' * 60)
        print(f'\n📁 Database: {self.db_path}')

        if not self.sql_dir or not self.sql_dir.exists():
            print(f'\n⚠️  SQL directory not found: {self.sql_dir}')
            print('   Use --sql-dir or set TRACEPIPE_AI_ETL_DIR')
            return False

        print(f'📂 SQL Directory: {self.sql_dir}')

        sql_files = list(self.sql_dir.glob('*.sql'))
        if not sql_files:
            print(f'\n⚠️  No .sql files found')
            return False

        print(f'📄 Found {len(sql_files)} SQL file(s)')

        # Parse files
        all_table: List[TableLineage] = []
        all_column: List[ColumnLineage] = []

        for sql_file in sorted(sql_files):
            print(f'\n📄 Parsing: {sql_file.name}')
            tl, cl = self.parser.parse_file(sql_file)
            all_table.extend(tl)
            all_column.extend(cl)

        # Write to database
        print(f'\n💾 Writing to database...')

        with self._connect(read_only=False) as con:
            try:
                con.execute('BEGIN TRANSACTION')
                con.execute(f'CREATE SCHEMA IF NOT EXISTS {self.meta_schema}')

                con.execute(f'''
                    CREATE OR REPLACE TABLE {self.meta_schema}.table_lineage (
                        target_table VARCHAR, source_table VARCHAR, sql_text VARCHAR
                    )
                ''')
                con.execute(f'''
                    CREATE OR REPLACE TABLE {self.meta_schema}.column_lineage (
                        target_table VARCHAR, target_column VARCHAR,
                        source_table VARCHAR, source_column VARCHAR,
                        transformation_logic VARCHAR, sql_file_name VARCHAR
                    )
                ''')

                for tl in all_table:
                    con.execute(f'INSERT INTO {self.meta_schema}.table_lineage VALUES (?, ?, ?)',
                               [tl.target_table, tl.source_table, tl.sql_text])

                for cl in all_column:
                    con.execute(f'INSERT INTO {self.meta_schema}.column_lineage VALUES (?, ?, ?, ?, ?, ?)',
                               [cl.target_table, cl.target_column, cl.source_table,
                                cl.source_column, cl.transformation_logic, cl.sql_file_name])

                con.execute('COMMIT')
                print('   ✅ Success!')

            except Exception as e:
                con.execute('ROLLBACK')
                print(f'   ❌ Error: {e}')
                return False

        # Summary
        unique_tables = len(set(tl.target_table for tl in all_table))
        print()
        print('=' * 60)
        print('✨ BUILD COMPLETE!')
        print(f'   📊 Tables: {unique_tables}')
        print(f'   🔗 Table lineage: {len(all_table)}')
        print(f'   📝 Column lineage: {len(all_column)}')
        print('=' * 60)

        return True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(description='Build metadata from SQL files')
    parser.add_argument('--sql-dir', help='Directory containing SQL files')
    parser.add_argument('--db', help='Path to database')
    parser.add_argument('--meta-schema', default='meta', help='Metadata schema name')

    args = parser.parse_args()

    # Auto-detect paths
    db_path = args.db or os.getenv('TRACEPIPE_AI_DB_PATH') or os.getenv('DEBUG_AI_DB_PATH')
    sql_dir = args.sql_dir or os.getenv('TRACEPIPE_AI_ETL_DIR') or os.getenv('DEBUG_AI_ETL_DIR')

    if not db_path:
        try:
            from config.db_config import DB_PATH
            db_path = str(DB_PATH) if DB_PATH else None
        except:
            pass

    if not sql_dir:
        try:
            from config.db_config import ETL_DIR
            sql_dir = str(ETL_DIR) if ETL_DIR else None
        except:
            pass

    if not db_path:
        print('❌ Database path required! Use --db or set TRACEPIPE_AI_DB_PATH')
        sys.exit(1)

    builder = MetadataBuilder(db_path=db_path, sql_dir=sql_dir, meta_schema=args.meta_schema)
    success = builder.build()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()