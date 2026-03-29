# Tracepipe AI

Open-source data lineage and quality platform with historical persistence.

## Features

### Historical Lineage & Time Travel

Databricks Unity Catalog retains lineage metadata for only 365 days. Tracepipe AI provides:

- **Persistent Storage**: Export and store lineage data beyond the 1-year limit
- **Time Travel Queries**: Query lineage state at any historical point
- **Evolution Tracking**: Visualize how data pipelines changed over time
- **Compliance Ready**: Meet long-term auditing and regulatory requirements

## Installation

bash
pip install -r requirements.txt


## Usage

### Export Lineage Data

bash
python -m scripts.cli export-lineage --source-csv lineage.csv


### Time Travel to Historical State

bash
python -m scripts.cli time-travel --date 2023-01-15T00:00:00 --table catalog.schema.table1


### View Lineage Evolution

bash
python -m scripts.cli lineage-evolution --table catalog.schema.table1 \
  --start-date 2023-01-01T00:00:00 --end-date 2024-01-01T00:00:00


## Architecture

- **DuckDB**: Embedded analytical database for lineage storage
- **Indexed Queries**: Fast time-based and table-based lookups
- **JSON Metadata**: Flexible schema for diverse lineage information

## License

See LICENSE file.
