# TracePipe AI

Open-source data observability and lineage tracking for modern data teams.

## Features

### 🔍 Advanced Impact & Root Cause Analysis

Understand the full blast radius of changes and identify root causes of data quality issues:

- **Downstream Impact Analysis (Blast Radius)**: Visualize all downstream dependencies before making changes
- **Upstream Root Cause Analysis**: Trace data quality issues back to their source
- **Change Simulation**: Predict the impact of schema changes, data modifications, or asset removal
- **Data Quality Diagnosis**: Automatically identify potential root causes of data issues

### 📊 Data Lineage Graph

- Automatically builds comprehensive lineage from database metadata
- Tracks table, view, and column-level dependencies
- Supports DuckDB with extensibility for other databases

### 🛡️ Senior Peer Review System

- Predicts technical failures from code changes
- Semantic delta extraction
- Technical validation

## Installation

bash
git clone https://github.com/yourusername/tracepipe_ai.git
cd tracepipe_ai
pip install -r requirements.txt


## Quick Start

### Impact Analysis (Blast Radius)

Analyze the downstream impact of changing a table:

bash
python -m scripts.cli impact "raw.companies"


Simulate a schema change:

bash
python -m scripts.cli impact "raw.companies" --simulate --change-type schema


### Root Cause Analysis

Find upstream dependencies that might cause issues:

bash
python -m scripts.cli rootcause "conformed.final_employee_stats"


Diagnose a data quality issue:

bash
python -m scripts.cli rootcause "conformed.final_employee_stats" --diagnose


### Lineage Graph

View the complete lineage graph:

bash
python -m scripts.cli lineage


Export to Graphviz DOT format:

bash
python -m scripts.cli lineage --export lineage.dot


## Usage Examples

### Understanding Blast Radius

Before making a schema change to `raw.companies`, check what will be affected:

bash
python -m scripts.cli impact "raw.companies" --json


Output shows:
- All affected downstream tables and views
- Dependency paths showing how changes propagate
- Risk level assessment
- Recommended actions

### Investigating Data Quality Issues

When a report shows incorrect data in `conformed.final_employee_stats`:

bash
python -m scripts.cli rootcause "conformed.final_employee_stats" --diagnose


Output provides:
- All upstream source tables and columns
- Critical dependencies to investigate first
- Prioritized investigation plan
- Recommendations for validation

## Architecture


tracepipe_ai/
├── scripts/
│   ├── cli.py                      # Command-line interface
│   ├── impact_analysis/            # Impact & root cause analysis
│   │   ├── lineage_graph.py       # Builds dependency graph
│   │   ├── impact_analyzer.py     # Downstream impact analysis
│   │   ├── root_cause_analyzer.py # Upstream root cause analysis
│   │   └── visualizer.py          # Graph visualization
│   └── peer_review/               # Code review system
└── config/                        # Configuration


## Features in Detail

### Lineage Graph Builder

- Extracts metadata from database information schema
- Parses SQL view definitions to understand transformations
- Builds directed graph with NetworkX
- Supports table, view, and column-level lineage

### Impact Analyzer

- Traverses graph downstream from source asset
- Calculates blast radius with configurable depth
- Assesses risk level (NONE, LOW, MEDIUM, HIGH, CRITICAL)
- Generates actionable recommendations
- Supports change simulation for different scenarios

### Root Cause Analyzer

- Traverses graph upstream to find dependencies
- Identifies critical (direct) vs. indirect dependencies
- Finds common root causes across multiple failing assets
- Prioritizes investigation based on dependency relationships
- Provides data quality diagnosis with recommendations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

## Support

For questions and support, please open an issue on GitHub.
