import argparse
import sys
from pathlib import Path
from config.workspace_config import WorkspaceConfig
from scripts.lineage_unification import LineageIngestion, LineageUnifier

def add_workspace_command(args):
    config = WorkspaceConfig()
    config.add_workspace(
        name=args.name,
        host=args.host,
        token=args.token,
        metastore_id=args.metastore_id,
        workspace_id=args.workspace_id
    )
    print(f"Workspace '{args.name}' added successfully")

def sync_lineage_command(args):
    config = WorkspaceConfig()
    workspaces = config.get_all_workspaces()
    if not workspaces:
        print("No workspaces configured. Add workspaces first.")
        return
    
    unifier = LineageUnifier()
    for workspace in workspaces:
        print(f"Fetching lineage from {workspace['name']}...")
        ingestion = LineageIngestion(workspace)
        lineage_data = ingestion.fetch_lineage()
        unifier.add_lineage_data(lineage_data)
        print(f"  Found {len(lineage_data)} lineage edges")
    
    graph = unifier.get_unified_graph()
    print(f"\nUnified lineage graph: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")

def query_lineage_command(args):
    config = WorkspaceConfig()
    workspaces = config.get_all_workspaces()
    unifier = LineageUnifier()
    
    for workspace in workspaces:
        ingestion = LineageIngestion(workspace)
        lineage_data = ingestion.fetch_lineage()
        unifier.add_lineage_data(lineage_data)
    
    paths = unifier.find_cross_workspace_lineage(args.pattern)
    print(f"Found {len(paths)} cross-workspace lineage connections:")
    for source, target in paths:
        print(f"  {source} -> {target}")

def main():
    parser = argparse.ArgumentParser(description="Tracepipe AI CLI")
    subparsers = parser.add_subparsers(dest='command')
    
    add_ws = subparsers.add_parser('add-workspace', help='Add Databricks workspace')
    add_ws.add_argument('--name', required=True)
    add_ws.add_argument('--host', required=True)
    add_ws.add_argument('--token', required=True)
    add_ws.add_argument('--metastore-id', required=True)
    add_ws.add_argument('--workspace-id', required=True)
    
    subparsers.add_parser('sync-lineage', help='Sync lineage from all workspaces')
    
    query = subparsers.add_parser('query-lineage', help='Query unified lineage')
    query.add_argument('--pattern', required=True)
    
    args = parser.parse_args()
    
    if args.command == 'add-workspace':
        add_workspace_command(args)
    elif args.command == 'sync-lineage':
        sync_lineage_command(args)
    elif args.command == 'query-lineage':
        query_lineage_command(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
