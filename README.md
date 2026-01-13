# 🔍 Debug AI - Talk to Your Data Lake!

Ask questions about your data in plain English:

> *"How is the risk_level column calculated?"*

> *"What tables feed into the sales report?"*

> *"Why is this customer marked as high risk?"*

And get answers based on your **actual SQL transformations**!

---

## 📁 What You Need

- ✅ A database: `.duckdb` file **OR** Databricks workspace
- ✅ SQL files with your transformations (`CREATE TABLE ... AS SELECT ...`)
- ✅ Python 3.8+
- ✅ Cursor or Claude Desktop

---

## 🚀 Quick Start

### 1. Install

```powershell
pip install duckdb pandas mcp
```

### 2. Setup (point to YOUR database and SQL files)

```powershell
python scripts/cli.py init --db "C:\path\to\your\database.duckdb" --sql "C:\path\to\sql\files"
python scripts/cli.py build
python scripts/cli.py test
```

### 3. Connect to Cursor

Add to Cursor settings, then restart Cursor:
```json
{
  "mcpServers": {
    "debug-ai": {
      "command": "python",
      "args": ["C:\\path\\to\\debug_ai\\mcp_server.py"]
    }
  }
}
```

### 4. Start Asking Questions!

Open Cursor and chat with your data.

---

## 📖 Full Setup Guide

### STEP 1: Download & Install (5 minutes)

#### 1.1 Download the Project

**Option A: With Git**
```powershell
git clone https://github.com/YOUR_USERNAME/debug_ai.git
cd debug_ai
```

**Option B: Without Git**
1. Download ZIP from GitHub
2. Unzip it
3. Open PowerShell:
```powershell
cd C:\Users\YourName\Downloads\debug_ai
```

#### 1.2 Install Python Packages

```powershell
pip install duckdb pandas mcp
```

Wait for "Successfully installed..." message.

#### 1.3 Verify It Works

```powershell
python scripts/cli.py --help
```

You should see a list of commands.

---

### STEP 2: Configure Your Database (2-5 minutes)

Choose your database type:

#### Option A: DuckDB (Local Database)

You need TWO paths:
- **Database path** - where your `.duckdb` file is
- **SQL folder path** - where your SQL transformation files are

```powershell
python scripts/cli.py init --db "C:\YOUR\PATH\database.duckdb" --sql "C:\YOUR\PATH\sql_folder"
```

**Real example:**
```powershell
python scripts/cli.py init --db "D:\DataLake\warehouse.duckdb" --sql "D:\DataLake\etl\transformations"
```

You should see:
```
✅ Database: D:\DataLake\warehouse.duckdb
✅ SQL Directory: D:\DataLake\etl\transformations (15 files)
✅ Configuration saved
```

#### Option B: Databricks (Cloud Data Platform) ☁️

**Step 2B.1: Install Databricks connector**
```powershell
pip install databricks-sql-connector
```

**Step 2B.2: Edit `config/config.yml`**

```yaml
# Set database type to databricks
db_type: "databricks"

# Databricks Configuration  
databricks:
  host: "your-workspace.cloud.databricks.com"    # Without https://
  token: "dapi..."                                # Your Personal Access Token
  http_path: "/sql/1.0/warehouses/abc123"         # From SQL Warehouse connection details
  catalog: "your_catalog"                         # e.g., main, hive_metastore, etc.
```

**Where to find your credentials:**
| Setting | Location in Databricks |
|---------|------------------------|
| `host` | Your workspace URL (e.g., `dbc-xxxxx.cloud.databricks.com`) |
| `token` | **User Settings → Developer → Access Tokens → Generate New Token** |
| `http_path` | **SQL Warehouses → [Your Warehouse] → Connection Details** |
| `catalog` | **Data → Catalog** (Unity Catalog name) |

**Step 2B.3: Test the connection**
```powershell
python config/db_config.py
```

Expected output:
```
Database Configuration
==================================================
Database Type: databricks
Host: your-workspace.cloud.databricks.com
Catalog: your_catalog
HTTP Path: /sql/1.0/warehouses/xxx
Token: ***xxxx

✅ Configuration loaded successfully
Schemas found: ['raw', 'silver', 'gold']
```

---

### STEP 3: Configure Lineage Source

Choose how you want lineage metadata to be collected:

#### Option A: Local SQL Files (Default for DuckDB)

If you have your SQL/ETL files locally, parse them to build lineage metadata:

1. **Set in `config/config.yml`:**
   ```yaml
   lineage_source: "local"
   etl_dir: "path/to/your/sql/files"
   ```

2. **Run the build:**
   ```powershell
   python scripts/cli.py build
   ```

   You should see:
   ```
   📄 Parsing: 01_raw_to_silver.sql
     ✅ Found: silver.customers
        └─ Sources: raw.customers
     ✅ Found: silver.orders
        └─ Sources: raw.orders, raw.products
        └─ Computed: total_amount

   ✨ BUILD COMPLETE!
      📊 Tables: 5
      🔗 Table lineage: 8
      📝 Column lineage: 3
   ```

#### Option B: Databricks Unity Catalog (Automatic Lineage) ☁️

If you're using Databricks with Unity Catalog, lineage is tracked **automatically**!

1. **Set in `config/config.yml`:**
   ```yaml
   db_type: "databricks"
   lineage_source: "databricks"
   ```

2. **No build step needed!** Unity Catalog tracks lineage automatically when:
   - Tables are created/updated via SQL or Spark
   - Jobs run on your cluster

3. **Test it:**
   ```python
   from scripts.debug_engine import DebugEngine
   engine = DebugEngine()
   
   # Get tables that feed into a target table
   sources = engine.get_upstream_tables('silver.customers')
   print(sources)
   ```

**Note:** Unity Catalog lineage requires:
- Unity Catalog enabled on your workspace
- Tables registered in a Unity Catalog
- Access to `system.access.table_lineage` and `system.access.column_lineage`

#### Option C: Auto-Detect

Let the system choose automatically:

```yaml
lineage_source: "auto"
etl_dir: "etl"  # Will use local if SQL files exist here
```

- If `etl_dir` has `.sql` files → uses **local** parsing
- If no SQL files but db_type is `databricks` → uses **Unity Catalog**

---

### STEP 4: Verify Everything Works (1 minute)

#### 4.1 Scan Your Database

```powershell
python scripts/cli.py scan
```

You should see:
```
📂 Schemas:
   • raw
   • silver
   • gold
   • meta          ← This should appear now!

📋 Tables:
   • meta.table_lineage (8 rows)    ← Metadata created!
   • meta.column_lineage (3 rows)   ← Metadata created!
   • raw.customers (1,000 rows)
   • silver.orders (5,000 rows)
   ...

📊 Metadata Status:
   ✅ table_lineage
   ✅ column_lineage
```

**Both must show ✅!**

#### 4.2 Run Full Test

```powershell
python scripts/cli.py test
```

All items should show ✅.

---

### STEP 5: Connect to Cursor (3 minutes)

Now the fun part - talk to your data!

#### 5.1 Get Your Project Path

Run this and **copy the output**:

```powershell
(Get-Location).Path
```

Example output: `C:\Users\John\debug_ai`

#### 5.2 Open Cursor Settings

1. Open Cursor
2. Press `Ctrl + Shift + P`
3. Type `settings json`
4. Click **"Preferences: Open User Settings (JSON)"**

#### 5.3 Add the MCP Server

Add this to your settings file (**replace the path with YOUR path from 5.1**):

```json
{
  "mcpServers": {
    "debug-ai": {
      "command": "python",
      "args": ["C:\\Users\\John\\debug_ai\\mcp_server.py"]
    }
  }
}
```

**⚠️ IMPORTANT:** Use double backslashes `\\` not single `\`

#### 5.4 Restart Cursor

Close Cursor completely and open it again.

---

### STEP 6: Start Talking! 🎉

In Cursor, you can now ask:

> "What tables do I have?"

> "What columns are in the customers table?"

> "How is the total_revenue column calculated?"

> "What tables feed into the sales_report?"

> "Why might the monthly_revenue be wrong?"

Cursor will use the MCP server to answer based on your actual data lineage!

---

## 📝 Recent Updates

See [UPDATE.md](UPDATE.md) for the latest features and enhancements.

---

## ⚙️ Database Configuration (`config/db_config.py`)

The `db_config.py` file provides flexible, auto-detecting configuration that works with multiple database types. It automatically finds your database and SQL files, or you can configure it manually.

### How It Works

**Auto-Detection (Default):**
The system automatically searches for:
- **Database files**: `*.duckdb` in common locations:
  - `data/*.duckdb`
  - `companies_data/*.duckdb`
  - `*.duckdb` (project root)
  - `database/*.duckdb`
  - `db/*.duckdb`

- **SQL/ETL directories**: Common folder names:
  - `etl/`
  - `sql/`
  - `transformations/`
  - `data/etl/`
  - `companies_data/etl/`
  - `dbt/models/`

### Configuration Methods

#### Method 1: Environment Variables (Recommended)

Set environment variables for explicit control:

**Windows (PowerShell):**
```powershell
$env:DEBUG_AI_DB_PATH = "C:\DataLake\warehouse.duckdb"
$env:DEBUG_AI_ETL_DIR = "C:\DataLake\etl"
$env:DEBUG_AI_DB_TYPE = "duckdb"
```

**Windows (Command Prompt):**
```cmd
set DEBUG_AI_DB_PATH=C:\DataLake\warehouse.duckdb
set DEBUG_AI_ETL_DIR=C:\DataLake\etl
set DEBUG_AI_DB_TYPE=duckdb
```

**Linux/Mac:**
```bash
export DEBUG_AI_DB_PATH="/path/to/warehouse.duckdb"
export DEBUG_AI_ETL_DIR="/path/to/etl"
export DEBUG_AI_DB_TYPE="duckdb"
```

#### Method 2: Default Locations

Simply place your files in standard locations:
- Database: `data/warehouse.duckdb` or `companies_data/corporate.duckdb`
- SQL files: `etl/` or `sql/` folder in project root

#### Method 3: Check Configuration

Verify what was detected:
```powershell
python config/db_config.py
```

Output example:
```
============================================================
📒 DEBUG AI - Configuration Check
============================================================

  ✅ project_root
     └─ C:\Users\John\debug_ai
  ✅ database
     └─ C:\Users\John\debug_ai\companies_data\corporate.duckdb
  ✅ etl_directory
     └─ C:\Users\John\debug_ai\companies_data\etl
  ℹ️  db_type: duckdb

============================================================
```

### Using Different Database Types

The system supports multiple database backends. Configure via `DEBUG_AI_DB_TYPE` environment variable.

#### DuckDB (Default)

No additional configuration needed if using file-based DuckDB:

```powershell
$env:DEBUG_AI_DB_PATH = "C:\Data\warehouse.duckdb"
$env:DEBUG_AI_DB_TYPE = "duckdb"  # Optional, this is the default
```

#### Databricks ✅ (Fully Supported)

Databricks is now fully integrated! Configure it in `config/config.yml`:

**Step 1: Edit `config/config.yml`**

```yaml
# Set database type to databricks
db_type: "databricks"

# Databricks Configuration
databricks:
  host: "your-workspace.cloud.databricks.com"
  token: "your-personal-access-token"
  http_path: "/sql/1.0/warehouses/your-warehouse-id"
  catalog: "your_catalog_name"
```

**Step 2: Get Your Databricks Credentials**

| Setting | Where to Find It |
|---------|------------------|
| `host` | Your Databricks workspace URL (without `https://`) |
| `token` | **User Settings → Developer → Access Tokens → Generate New Token** |
| `http_path` | **SQL Warehouses → Your Warehouse → Connection Details → HTTP Path** |
| `catalog` | The Unity Catalog you want to query (e.g., `hive_metastore`, `main`, etc.) |

**Step 3: Test the Connection**

```powershell
python config/db_config.py
```

Expected output:
```
Database Configuration
==================================================
Database Type: databricks
Host: your-workspace.cloud.databricks.com
Catalog: your_catalog
HTTP Path: /sql/1.0/warehouses/your-warehouse-id
Token: ***xxxx

✅ Configuration loaded successfully
Schemas found: ['schema1', 'schema2', 'schema3']
```

**Step 4: Install Databricks Connector (if not already installed)**

```powershell
pip install databricks-sql-connector
```

#### Snowflake

Set these environment variables:

```powershell
$env:DEBUG_AI_DB_TYPE = "snowflake"
$env:SNOWFLAKE_ACCOUNT = "your-account"
$env:SNOWFLAKE_USER = "your-username"
$env:SNOWFLAKE_PASSWORD = "your-password"
$env:SNOWFLAKE_WAREHOUSE = "your-warehouse"
$env:SNOWFLAKE_DATABASE = "your-database"
```

**Note:** Snowflake connector needs to be implemented in `scripts/debug_engine.py`. Currently, the structure supports it but requires implementation.

### Adding Custom Database Types

To add support for a new database type:

1. **Edit `config/db_config.py`:**
   Add a new branch in the `get_db_config()` function:

   ```python
   elif DB_TYPE == 'your_database':
       config['connection_details'] = {
           'host': os.getenv('YOUR_DB_HOST'),
           'port': os.getenv('YOUR_DB_PORT', '5432'),
           'database': os.getenv('YOUR_DB_NAME'),
           'user': os.getenv('YOUR_DB_USER'),
           'password': os.getenv('YOUR_DB_PASSWORD'),
       }
   ```

2. **Implement the connector in `scripts/debug_engine.py`:**
   Update the `DatabaseConnector` class to handle your database type, implementing:
   - Connection logic
   - Query execution
   - Schema/table discovery
   - Column type detection

3. **Set environment variable:**
   ```powershell
   $env:DEBUG_AI_DB_TYPE = "your_database"
   ```

### Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `DEBUG_AI_DB_PATH` | Path to database file (DuckDB) | `C:\Data\warehouse.duckdb` |
| `DEBUG_AI_ETL_DIR` | Path to SQL/ETL directory | `C:\Data\etl` |
| `DEBUG_AI_DB_TYPE` | Database type | `duckdb`, `databricks`, `snowflake` |
| `DATABRICKS_HOST` | Databricks workspace URL | `workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Databricks access token | `dapi...` |
| `DATABRICKS_WAREHOUSE_ID` | Databricks SQL warehouse ID | `abc123...` |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `xy12345` |
| `SNOWFLAKE_USER` | Snowflake username | `admin` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `password123` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | Snowflake database name | `PRODUCTION` |

---

## 📋 CLI Commands

| Command | What It Does |
|---------|--------------|
| `python scripts/cli.py init --db "..." --sql "..."` | Configure paths |
| `python scripts/cli.py build` | Parse SQL → Create metadata |
| `python scripts/cli.py scan` | Show tables & metadata status |
| `python scripts/cli.py test` | Verify everything works |
| `python scripts/cli.py query table column` | Quick lineage lookup |
| `python scripts/cli.py serve` | Start MCP server manually |

```

1. **build** parses your SQL files and stores lineage in metadata tables
2. **MCP server** exposes tools for AI to query that lineage
3. **Cursor/Claude** uses those tools to answer your questions!

---

## 🆘 Troubleshooting

### "Python not found"
→ Install Python from https://python.org
→ Make sure to check "Add to PATH" during install

### "No module named duckdb"
→ Run: `pip install duckdb pandas mcp`

### "Database not found"
→ Check your path is correct
→ Make sure the file exists
→ Use full path like `C:\Users\...` not relative path

### "0 tables found" after build
→ Check your SQL files have `CREATE TABLE` or `CREATE OR REPLACE TABLE`
→ Make sure SQL folder path is correct
→ Check SQL files end with `.sql`

### Cursor not connecting
→ Did you restart Cursor after adding config?
→ Check path uses double backslashes `\\`
→ Try running `python mcp_server.py` manually to see errors

### "No lineage found"
→ Run `python scripts/cli.py build` first
→ Run `python scripts/cli.py scan` and check metadata shows ✅

---

## ✅ Success Checklist

Before asking for help, verify:

- [ ] Python works: `python --version`
- [ ] Packages installed: `pip install duckdb pandas mcp`
- [ ] Init done: `python scripts/cli.py init --db "..." --sql "..."`
- [ ] Build done: `python scripts/cli.py build` shows tables found
- [ ] Scan shows: `✅ table_lineage` and `✅ column_lineage`
- [ ] Test passes: `python scripts/cli.py test` shows all ✅
- [ ] Cursor config has correct path with `\\`
- [ ] Cursor was restarted

---

## 🎬 Complete Setup (Copy-Paste)

```powershell
# 1. Install packages
pip install duckdb pandas mcp

# 2. Initialize (CHANGE THESE PATHS!)
python scripts/cli.py init --db "C:\YOUR\database.duckdb" --sql "C:\YOUR\sql_folder"

# 3. Build metadata
python scripts/cli.py build

# 4. Verify
python scripts/cli.py scan
python scripts/cli.py test

# 5. Show path for Cursor config
Write-Host "Add to Cursor settings:"
Write-Host "mcp_server path: $((Get-Location).Path -replace '\\', '\\')\\mcp_server.py"
```

Then add to Cursor settings and restart Cursor!

---

Made with ❤️ for Data Engineers
