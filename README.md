# Debug AI

**Data lineage debugger and peer review system for SQL pipelines.**

Debug AI connects to your database, traces how every table and column is built, and catches mistakes before they reach production. It has two main features:

1. **Lineage Engine** — Trace how any column is calculated, all the way back to source tables
2. **Peer Review** — Automatically check your SQL changes for syntax errors and downstream impact

---

## Installation

### 1. Clone and set up

```bash
git clone <your-repo-url>
cd debug_ai
```

### 2. Create virtual environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac/Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure your database

Edit `config/config.yml` — set `db_type` and your database path:

```yaml
# For local DuckDB:
db_type: "duckdb"
duckdb:
  path: "companies_data_duckdb/corporate.duckdb"

# For Databricks: see "Connections" section below
```

### 5. Build lineage metadata

This parses your SQL files and builds the lineage graph:

```bash
python scripts/cli.py build
```

You only need to run this once, and again when you add/change SQL files.

---

## Feature 1: Lineage Engine

The lineage engine traces how data flows through your SQL pipeline — from raw source tables through silver/conformed layers.

### How to use

**List all tables:**

```bash
python scripts/cli.py scan
```

**Trace how a column is calculated:**

```bash
python scripts/cli.py query conformed.churn_risk risk_level
```

This shows the full chain: which source tables feed into it, what SQL logic builds it, and where the code is defined.

**Get upstream sources for a table:**

```bash
python scripts/cli.py query --sources conformed.churn_risk
```

**Get full lineage tree:**

```bash
python scripts/cli.py query --tree conformed.churn_risk
```

### Example output

```
python scripts/cli.py query conformed.churn_risk risk_level

🔍 Column Lineage Report: conformed.churn_risk.risk_level

📊 Column Definition:
   CASE
     WHEN cs.total_jobs >= 3 THEN 'HIGH (Job Hopper)'
     WHEN cs.peak_salary < 50000 THEN 'HIGH (Underpaid)'
     ELSE 'LOW'
   END AS risk_level

📦 Source Tables:
   • conformed.career_summary → cs.total_jobs, cs.peak_salary
   • silver.dim_employees → e.full_name

📁 Defined in: companies_data_duckdb/etl/01_corporate_logic.sql
```

### MCP Server (for AI tools)

Both features work via MCP so AI tools like Claude can use them from chat:

```bash
python mcp_server.py
```

Once running, just ask your AI tool:
- *"Explain how risk_level is calculated"* → uses the lineage engine
- *"Peer review my changes"* → runs the full peer review and shows the report

---

## Feature 2: Peer Review

The peer review checks your SQL changes for:

1. **Syntax errors** — unknown schemas, keyword typos, unmatched parentheses, unclosed strings, missing FROM/GROUP BY, trailing commas, duplicate aliases
2. **Impact chain** — shows which tables you changed and which downstream tables will be affected (the "domino effect")

### What it catches

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

### How to use

**From chat (MCP):** Just ask your AI tool *"run peer review"* or *"check my code"*.

**From terminal:**

```bash
python scripts/cli.py peer-review check
```

Both methods compare your changed files (including new untracked files) against the last git commit and show the impact.

### Options

```bash
# Only check git-staged files:
python scripts/cli.py peer-review check --staged-only

# Save report as JSON:
python scripts/cli.py peer-review check --output report.json

# Block commit on RED risk (for git hooks):
python scripts/cli.py peer-review check --block
```

### Install as git hook

Run automatically before every commit:

```bash
python scripts/cli.py peer-review install-hook
```

### Example: 🟡 YELLOW (logic change with downstream impact)

```
╔====================================================================╗
║                    SENIOR PEER REVIEW                              ║
╚====================================================================╝

Risk Level: 🟡 YELLOW - Proceed with caution

📋 Directly Changed Tables:
   • conformed.career_summary  — Added WHERE salary > 80000
   • conformed.company_stats  — AVG() changed to SUM()

🔗 Impact Chain (Downstream Dominos):
   conformed.career_summary
     └─→ conformed.churn_risk (1 hop - direct dependency)

   conformed.company_stats
     └─→ conformed.total_compensation (1 hop - direct dependency)
```

### Example: 🔴 RED (syntax error caught)

```
╔====================================================================╗
║                    SENIOR PEER REVIEW                              ║
╚====================================================================╝

Risk Level: 🔴 RED - Manual review required

⚠️  Syntax Errors:
   🔴 Unknown schema 'silv' at line 3 (did you mean 'silver'?)

📋 Directly Changed Tables:
   • silv.dim_departments  — NEW TABLE added

💡 Advisory:
   MANUAL REVIEW REQUIRED
   1. Fix syntax errors before committing
```

### Risk levels

| Level | What it means |
|-------|--------------|
| 🟢 GREEN | Safe to commit. No downstream impact. |
| 🟡 YELLOW | Downstream tables will be affected. Review the impact chain. |
| 🔴 RED | Syntax errors found, or many tables affected. Fix before deploying. |

---

## Connections

Debug AI supports two database types: **DuckDB** (local) and **Databricks**.

### DuckDB (local)

DuckDB is the default — no server needed. Everything runs on your machine.

**Setup in `config/config.yml`:**

```yaml
db_type: "duckdb"
lineage_source: "local"
sql_dir: "companies_data_duckdb/etl"

duckdb:
  path: "companies_data_duckdb/corporate.duckdb"
```

**Populate sample data (optional):**

```bash
python companies_data_duckdb/setup_raw.py
```

**Build lineage:**

```bash
python scripts/cli.py build
```

### Databricks

Connect to a Databricks workspace with Unity Catalog for automatic lineage tracking.

**Setup in `config/config.yml`:**

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

1. **Host**: Your Databricks workspace URL (without `https://`)
2. **Token**: Go to Settings → Developer → Access Tokens → Generate New Token
3. **HTTP Path**: Go to SQL Warehouses → your warehouse → Connection Details → HTTP Path
4. **Catalog**: The Unity Catalog name (e.g., `companies_data`)

**Test the connection:**

```bash
python scripts/cli.py scan
```

This should list all schemas and tables in your catalog.

> **Note:** With Databricks + Unity Catalog, lineage is tracked automatically from any query that runs — no need to run `build` manually.

---

## Project Structure

```
debug_ai/
├── config/
│   ├── config.yml          # Main configuration
│   └── db_config.py        # Database config loader
├── scripts/
│   ├── cli.py              # Main CLI entry point
│   ├── debug_engine.py     # Core lineage engine
│   ├── build_metadata.py   # Builds lineage metadata from SQL files
│   └── peer_review/        # Peer review system
│       ├── peer_review.py      # Orchestrator
│       ├── semantic_delta.py   # Git diff analyzer
│       ├── blast_radius.py     # Downstream impact tracer
│       └── technical_validator.py  # Syntax checker
├── companies_data_duckdb/  # Sample DuckDB project
│   ├── etl/                # SQL transformation files
│   └── setup_raw.py        # Sample data generator
├── mcp_server.py           # MCP server for AI tools
└── requirements.txt        # Python dependencies
```
