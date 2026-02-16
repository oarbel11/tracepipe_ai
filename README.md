# Debug AI

**Data lineage debugger and peer review for SQL pipelines.**

Debug AI connects to your database, traces how every table and column is built, and catches mistakes before they reach production.

### Features

1. **Lineage Engine** — Ask any question about your data: *"How is risk_level calculated?"* and get a full trace back to the source tables, including the SQL logic and where it's defined.

2. **Peer Review** — Get a senior-level review on your code changes before you commit. It scans your SQL scripts and jobs for syntax errors, identifies what tables you modified, and shows which downstream tables will be affected — like having a senior engineer review every change.

---

## Installation

```bash
# 1. Clone
git clone <your-repo-url>
cd debug_ai

# 2. Virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt
```

### Configure your database

Edit `config/config.yml`:

```yaml
# For local DuckDB:
db_type: "duckdb"
duckdb:
  path: "companies_data_duckdb/corporate.duckdb"

# For Databricks: see "Connections" section below
```

### Build lineage

```bash
python scripts/cli.py build
```

Run this once, and again whenever you add or change SQL files.

---

## Usage

Both features work via **MCP** (for AI tools like Claude, Cursor, etc.) and via the **CLI**.

### MCP server (for AI tools)

1. Add this to your AI tool's MCP config (Cursor, Claude Desktop, etc.):

```json
{
  "mcpServers": {
    "debug-ai": {
      "command": "python",
      "args": ["/full/path/to/debug_ai/mcp_server.py"]
    }
  }
}
```

2. Restart your AI tool, then just ask in chat:

- *"How is risk_level calculated?"* → lineage trace
- *"Run peer review"* → full change review with risk level

### CLI commands

```bash
# Lineage
python scripts/cli.py query conformed.churn_risk risk_level   # Trace a column
python scripts/cli.py scan                                    # List all tables

# Peer Review
python scripts/cli.py peer-review check                       # Review changes
python scripts/cli.py peer-review check --staged-only         # Only staged files
python scripts/cli.py peer-review check --block               # Block on RED risk
python scripts/cli.py peer-review install-hook                # Auto-run on commits
```

---

## Peer Review: What it catches

| Check | Example |
|-------|---------|
| Unknown schema | `silv.my_table` → *"did you mean 'silver'?"* |
| Keyword typos | `SELCT`, `FRON`, `JOUN`, `WEHRE`, `FORM` |
| Unmatched parentheses | `COUNT(salary` missing `)` |
| Unclosed strings | `WHERE name = 'Alice` |
| Missing FROM | `SELECT t.col` with no `FROM` |
| Aggregation without GROUP BY | `SUM(salary)` with plain columns |
| Trailing comma | `col1, FROM table` |
| Duplicate aliases | Two columns both `AS total` |
| Downstream impact | Shows every table affected by your change |

### Risk levels

| Level | Meaning |
|-------|---------|
| 🟢 GREEN | Safe to commit. No downstream impact. |
| 🟡 YELLOW | Downstream tables affected. Review the impact chain. |
| 🔴 RED | Syntax errors or high impact. Fix before deploying. |

### Example output

```
╔====================================================================╗
║                    SENIOR PEER REVIEW                              ║
╚====================================================================╝

Risk Level: 🔴 RED - Manual review required

⚠️  Syntax Errors:
   🔴 Unknown schema 'silv' at line 3 (did you mean 'silver'?)

📋 Directly Changed Tables:
   • conformed.churn_risk  — Added 1 JOIN(s)
   • silv.dim_departments  — NEW TABLE added

� Impact Chain (Downstream Dominos):
   conformed.career_summary
     └─→ conformed.churn_risk (1 hop - direct dependency)

💡 Advisory:
   MANUAL REVIEW REQUIRED
   1. Fix syntax errors before committing
```

---

## Connections

### DuckDB (local)

No server needed — everything runs on your machine.

```yaml
db_type: "duckdb"
lineage_source: "local"
sql_dir: "companies_data_duckdb/etl"

duckdb:
  path: "companies_data_duckdb/corporate.duckdb"
```

### Databricks

Connect to a Databricks workspace with Unity Catalog.

```yaml
db_type: "databricks"
lineage_source: "databricks"

databricks:
  host: "your-workspace.cloud.databricks.com"
  token: "dapi..."
  http_path: "/sql/1.0/warehouses/your-warehouse-id"
  catalog: "your_catalog"
```

**How to get your connection details:**

1. **Host** — Your workspace URL (without `https://`)
2. **Token** — Settings → Developer → Access Tokens → Generate New Token
3. **HTTP Path** — SQL Warehouses → your warehouse → Connection Details
4. **Catalog** — Your Unity Catalog name (e.g., `companies_data`)

> With Databricks + Unity Catalog, lineage is tracked automatically — no need to run `build`.

---

## Project Structure

```
debug_ai/
├── config/
│   ├── config.yml              # Main configuration
│   └── db_config.py            # Database config loader
├── scripts/
│   ├── cli.py                  # CLI entry point
│   ├── debug_engine.py         # Lineage engine
│   ├── build_metadata.py       # Builds lineage from SQL
│   └── peer_review/            # Peer review system
│       ├── peer_review.py          # Orchestrator
│       ├── semantic_delta.py       # Git diff analyzer
│       ├── blast_radius.py         # Downstream impact tracer
│       └── technical_validator.py  # Syntax checker
├── companies_data_duckdb/      # Sample DuckDB project
├── mcp_server.py               # MCP server for AI tools
└── requirements.txt            # Python dependencies
```
