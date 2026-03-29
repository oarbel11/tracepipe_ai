# Databricks Pipeline Lineage Extractor

Extracts end-to-end lineage from Databricks assets including notebooks, jobs,
Delta Live Tables pipelines, and data tables.

## Setup

```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"
```

## Usage

```bash
python -m scripts.databricks_lineage.cli
```

## Features

- Extracts lineage from Databricks jobs and notebooks
- Parses SQL queries to identify table dependencies
- Generates graph with nodes (jobs, notebooks, tables) and edges
- Supports Delta Live Tables pipeline metadata

## Output Format

JSON with:
- `nodes`: List of assets (type, id, name)
- `edges`: List of dependencies (source, target, type)
