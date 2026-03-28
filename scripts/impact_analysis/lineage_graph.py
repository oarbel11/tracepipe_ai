"""
Lineage Graph Builder

Builds a directed graph of data dependencies from database metadata.
"""

import networkx as nx
import duckdb
import sqlparse
import re
from typing import Dict, List, Set, Tuple, Optional


class LineageGraphBuilder:
    """Builds lineage graph from database metadata"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.graph = nx.DiGraph()
        self.table_columns = {}
        self.view_definitions = {}
        
    def build_graph(self) -> nx.DiGraph:
        """Build complete lineage graph from database metadata"""
        con = duckdb.connect(self.db_path, read_only=True)
        
        try:
            # Get all tables and views
            self._extract_tables(con)
            self._extract_views(con)
            self._extract_column_lineage(con)
            
        finally:
            con.close()
            
        return self.graph
    
    def _extract_tables(self, con: duckdb.DuckDBPyConnection):
        """Extract all tables and their columns"""
        tables = con.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position
        """).fetchall()
        
        for schema, table, column, dtype in tables:
            table_full = f"{schema}.{table}"
            column_full = f"{schema}.{table}.{column}"
            
            # Add table node
            if table_full not in self.graph:
                self.graph.add_node(table_full, type='table', schema=schema, name=table)
                self.table_columns[table_full] = []
            
            # Add column node
            self.graph.add_node(column_full, type='column', schema=schema, 
                              table=table, column=column, data_type=dtype)
            self.table_columns[table_full].append(column)
            
            # Link column to table
            self.graph.add_edge(column_full, table_full, relationship='belongs_to')
    
    def _extract_views(self, con: duckdb.DuckDBPyConnection):
        """Extract views and parse their SQL definitions"""
        try:
            views = con.execute("""
                SELECT table_schema, table_name, view_definition
                FROM information_schema.views
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """).fetchall()
            
            for schema, view_name, view_def in views:
                view_full = f"{schema}.{view_name}"
                self.graph.add_node(view_full, type='view', schema=schema, name=view_name)
                self.view_definitions[view_full] = view_def
                
                # Parse view definition to find dependencies
                dependencies = self._parse_sql_dependencies(view_def)
                for dep_table in dependencies:
                    if dep_table in self.graph:
                        self.graph.add_edge(dep_table, view_full, relationship='feeds_into')
        except Exception as e:
            # Views table might not exist in all DuckDB versions
            pass
    
    def _extract_column_lineage(self, con: duckdb.DuckDBPyConnection):
        """Extract column-level lineage from view definitions"""
        for view_full, view_def in self.view_definitions.items():
            schema, view_name = view_full.split('.')
            
            # Get columns for this view
            try:
                view_columns = con.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{view_name}'
                """).fetchall()
                
                for (col_name,) in view_columns:
                    col_full = f"{view_full}.{col_name}"
                    self.graph.add_node(col_full, type='column', schema=schema,
                                      table=view_name, column=col_name)
                    self.graph.add_edge(col_full, view_full, relationship='belongs_to')
                    
                    # Try to find source columns
                    source_cols = self._find_source_columns(view_def, col_name)
                    for src_col in source_cols:
                        if src_col in self.graph:
                            self.graph.add_edge(src_col, col_full, relationship='transforms_to')
            except Exception:
                pass
    
    def _parse_sql_dependencies(self, sql: str) -> Set[str]:
        """Parse SQL to find table dependencies"""
        dependencies = set()
        
        # Simple regex-based parsing for FROM and JOIN clauses
        # Match schema.table or just table
        pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        
        for match in matches:
            # If no schema, try to infer or skip
            if '.' in match:
                dependencies.add(match)
            else:
                # Check if table exists in any schema
                for table_full in self.table_columns.keys():
                    if table_full.endswith(f".{match}"):
                        dependencies.add(table_full)
                        break
        
        return dependencies
    
    def _find_source_columns(self, sql: str, target_column: str) -> Set[str]:
        """Find source columns for a target column in SQL"""
        source_columns = set()
        
        # This is a simplified version - real implementation would need full SQL parsing
        # Look for column references in SELECT clause
        pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, sql)
        
        for match in matches:
            if match in self.graph and self.graph.nodes[match].get('type') == 'column':
                source_columns.add(match)
        
        return source_columns
    
    def get_asset_node(self, asset_identifier: str) -> Optional[str]:
        """Find asset node in graph by various identifiers"""
        # Direct match
        if asset_identifier in self.graph:
            return asset_identifier
        
        # Partial match (e.g., just table name)
        for node in self.graph.nodes():
            if node.endswith(f".{asset_identifier}") or node.endswith(asset_identifier):
                return node
        
        return None
