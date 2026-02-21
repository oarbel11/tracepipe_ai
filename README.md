# Tracepipe AI

**Data lineage debugger and peer review for SQL pipelines.**

Tracepipe AI connects to your database, traces how every table and column is built, and catches mistakes before they reach production.

---

## 🚀 Quick Start

### Install (run in your IDE terminal)

**Mac / Linux:**
```bash
curl -fsSL https://raw.githubusercontent.com/oarbel11/tracepipe_ai/master/install/install.sh | bash
```

**Windows (any IDE terminal — VS Code, Cursor, etc.):**
```powershell
curl.exe -fsSL "https://raw.githubusercontent.com/oarbel11/tracepipe_ai/master/install/install.ps1" -o install.ps1; powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1
```
This downloads the installer and runs it. The window stays open so you can read any errors.

### What Happens Next

After running the install command, you'll go through an **interactive setup wizard** that will:

1. **Ask which database you're using:**
   - DuckDB (local file-based database)
   - Databricks (cloud workspace)

2. **Collect connection details:**
   - **For DuckDB:** Path to your database file
   - **For Databricks:** Host, token, HTTP path, and catalog name

3. **Ask for your ETL files location:**
   - Path to your SQL/ETL files directory (where your CREATE TABLE statements are)

4. **Automatically configure MCP** (if Cursor or Claude Desktop detected):
   - Detects installed AI tools
   - Automatically writes MCP configuration files
   - Shows manual instructions if no tools detected

5. **Guide you through next steps:**
   - Build lineage metadata
   - Set up peer review

The wizard automatically writes your configuration to `config/config.yml`. You can edit this file anytime to change settings.

---

## 📖 What is Tracepipe AI?

Tracepipe AI has two main features:

### 1. Lineage Engine
Ask questions about your data and get full traces back to source tables.

**Example:** *"How is risk_level calculated?"*

You'll get:
- The SQL logic that calculates it
- Which tables it comes from
- The full dependency chain

### 2. Peer Review
Get senior-level code reviews before you commit. It catches:
- SQL syntax errors (typos, missing parentheses, etc.)
- What tables you changed
- Which downstream tables will be affected
- Business logic issues (grain mismatches)

---

## 💻 Usage

### Via AI Tools (MCP)

The setup wizard **automatically configures MCP** for Cursor and Claude Desktop if detected.

**If auto-configuration didn't run or you need to configure manually:**

1. **Add to your AI tool's MCP config** (Cursor, Claude Desktop, etc.):
   ```json
   {
     "mcpServers": {
       "debug-ai": {
         "command": "python",
         "args": ["/full/path/to/tracepipe_ai/mcp_server.py"]
       }
     }
   }
   ```
   > **Note:** Run `python scripts/cli.py setup` to see the exact path and configuration.

2. **Restart your AI tool**, then ask:
   - *"How is risk_level calculated?"* → lineage trace
   - *"Run peer review"* → full change review

### Via CLI

**Setup & Configuration:**
```bash
python scripts/cli.py setup                              # Run setup wizard (configure database & ETL paths)
python scripts/cli.py config --show                     # Show current configuration
```

**Lineage:**
```bash
python scripts/cli.py build                              # Build metadata (run after adding/changing SQL)
python scripts/cli.py query conformed.churn_risk risk_level   # Trace a column
python scripts/cli.py scan                              # List all tables
```

**Peer Review:**
```bash
python scripts/cli.py peer-review setup                 # Build business context (run once)
python scripts/cli.py peer-review check                  # Review all changes
python scripts/cli.py peer-review check --staged-only  # Only staged files
python scripts/cli.py peer-review check --block         # Block commits on RED risk
python scripts/cli.py peer-review install-hook          # Auto-run on git commits
```

---

## 🔍 Peer Review: What It Catches

| Issue | Example |
|-------|---------|
| Unknown schema | `silv.my_table` → *"did you mean 'silver'?"* |
| Keyword typos | `SELCT`, `FRON`, `JOUN`, `WEHRE`, `FORM` |
| Unmatched parentheses | `COUNT(salary` missing `)` |
| Unclosed strings | `WHERE name = 'Alice` |
| Missing FROM | `SELECT t.col` with no `FROM` |
| Aggregation without GROUP BY | `SUM(salary)` with plain columns |
| SELECT/GROUP BY mismatch | `SELECT company_name` but `GROUP BY company_id` |
| Misleading aliases | `SUM(salary) AS avg_salary` |
| Trailing comma | `col1, FROM table` |
| Duplicate aliases | Two columns both `AS total` |
| Downstream impact | Shows every table affected by your change |

### Risk Levels

| Level | Meaning |
|-------|---------|
| 🟢 **GREEN** | Safe to commit. No downstream impact. |
| 🟡 **YELLOW** | Downstream tables affected. Review the impact chain. |
| 🔴 **RED** | Syntax errors or high impact. Fix before deploying. |

### Example Output

```
PEER REVIEW REPORT

SENIOR DATA ENGINEER NOTES:
  - You changed ETL script(s): companies_data_duckdb/etl/01_corporate_logic.sql
  - silver.fact_jobs feeds 5 downstream table(s). Confirm metrics stay aligned.

** conformed.company_stats
   Changed from: AVG(salary), GROUP BY company_name
   To: SUM(salary), GROUP BY company_id
   This change impacts: conformed.total_compensation

INCORRECT (technical):
  - SELECT column 'company_name' not in GROUP BY (grouped by: COMPANY_ID)
  - Misleading alias: SUM(...) AS avg_salary — rename to total_salary
```

---

## 🔌 Database Configuration

### DuckDB (Local)

The setup wizard will ask for:
- **Database file path:** Path to your `.duckdb` file

Example configuration:
```yaml
db_type: "duckdb"
lineage_source: "local"
sql_dir: "/path/to/your/etl/files"
duckdb:
  path: "/path/to/your/database.duckdb"
```

### Databricks

The setup wizard will ask for:
- **Host:** Your workspace URL (e.g., `your-workspace.cloud.databricks.com`)
- **Token:** Access token from Settings → Developer → Access Tokens
- **HTTP Path:** From SQL Warehouses → Connection Details
- **Catalog:** Your Unity Catalog name

Example configuration:
```yaml
db_type: "databricks"
lineage_source: "databricks"
databricks:
  host: "your-workspace.cloud.databricks.com"
  token: "dapi..."
  http_path: "/sql/1.0/warehouses/your-warehouse-id"
  catalog: "your_catalog"
```

> **Note:** With Databricks + Unity Catalog, lineage is tracked automatically — no need to run `build`.

---

## 🛠️ Manual Installation

If you prefer not to use the installer:

```bash
git clone https://github.com/oarbel11/tracepipe_ai.git
cd tracepipe_ai
python -m venv .venv
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
python scripts/setup_wizard.py  # Run setup wizard manually
```

---

## ❓ Troubleshooting

### "Git is not installed"
- **Mac:** `brew install git`
- **Linux:** `sudo apt-get install git` (Ubuntu/Debian) or `sudo yum install git` (RHEL/CentOS)
- **Windows:** Download from https://git-scm.com/downloads/win

### "Python 3.8+ required"
- Install Python 3.8+ from https://www.python.org/downloads/
- Make sure `python3` (or `python` on Windows) is in your PATH

### Installation fails
- Check your internet connection
- Verify the repository URL is correct
- Try manual installation instead

### Setup wizard doesn't run
- Make sure you're in the `tracepipe_ai` directory
- Activate the virtual environment: `source .venv/bin/activate` (Mac/Linux) or `.venv\Scripts\activate` (Windows)
- Run manually: `python scripts/cli.py setup` or `python scripts/setup_wizard.py`

---

## 📁 Project Structure

```
tracepipe_ai/
├── config/                      # Configuration files
│   ├── config.yml              # Main configuration (auto-generated by wizard)
│   └── db_config.py            # Database config loader
├── install/                     # Installation scripts
│   ├── install.sh              # Unix/Mac installer
│   ├── install.ps1             # Windows installer
│   ├── setup_repo_url.sh       # Helper script for forks
│   └── setup_repo_url.ps1     # Helper script for forks (Windows)
├── scripts/                     # Core application code
│   ├── cli.py                  # CLI entry point
│   ├── setup_wizard.py         # Interactive setup wizard
│   ├── debug_engine.py         # Lineage engine
│   ├── build_metadata.py       # Builds lineage from SQL
│   └── peer_review/            # Peer review system
│       ├── peer_review.py          # Orchestrator
│       ├── semantic_delta.py       # Git diff analyzer
│       ├── blast_radius.py         # Downstream impact tracer
│       └── technical_validator.py  # Syntax checker
├── companies_data_duckdb/      # Sample DuckDB project
├── mcp_server.py               # MCP server for AI tools
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.
