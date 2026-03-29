"""CLI for Databricks lineage extraction."""
import os
import json
import sys
from .lineage_extractor import DatabricksLineageExtractor


def main():
    """Main CLI entry point."""
    workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
    token = os.getenv('DATABRICKS_TOKEN')
    
    if not workspace_url or not token:
        print('Error: DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN required',
              file=sys.stderr)
        sys.exit(1)
    
    extractor = DatabricksLineageExtractor(workspace_url, token)
    lineage = extractor.extract_lineage()
    
    print(json.dumps(lineage, indent=2))


if __name__ == '__main__':
    main()
