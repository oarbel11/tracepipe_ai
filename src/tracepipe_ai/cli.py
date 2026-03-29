"""Command-line interface for Tracepipe AI."""
import sys
import json
from typing import List
from src.tracepipe_ai.lineage_unification import (
    LineageUnifier,
    WorkspaceConfig,
)


def main(args: List[str] = None) -> int:
    """Main CLI entry point."""
    if args is None:
        args = sys.argv[1:]

    if not args or args[0] == "--help":
        print("Usage: tracepipe-ai <config.json>")
        return 0

    config_path = args[0]
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)

        workspaces = [
            WorkspaceConfig(**ws) for ws in config_data.get("workspaces", [])
        ]

        unifier = LineageUnifier(workspaces)
        graph = unifier.fetch_lineage()

        print(f"Unified lineage graph: {len(graph.nodes)} nodes")
        print(f"Edges: {len(graph.edges)}")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
