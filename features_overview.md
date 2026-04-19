# Tracepipe AI — Features Overview

This file lists features that are **confirmed working** (all tests passed).
Each entry explains what the feature does and how to use it.

---

## 📅 2026-04-05

### 🔍 Feature #1 — Data Lineage & Column Tracing

**What it is & why it matters:**
The core feature. Given any table and column in your data stack, Tracepipe AI traces exactly how that value is calculated — walking upstream through every transformation, JOIN, and aggregation all the way back to the raw source. For Databricks users, this reads live from Unity Catalog's `system.access.column_lineage`. For local setups it reads from the parsed metadata index. This answers the question engineers and analysts ask every day: *"Where does this number actually come from?"*

**How to use it:**
Just ask in the chat — no commands, no file paths needed.

- *"How is `risk_level` calculated in `conformed.churn_risk`?"*
- *"Where does `total_revenue` come from?"*
- *"Explain the `active_users` column in `gold.dashboard`"*

The AI calls `explain_column(target_table, target_column)` and returns the full transformation chain: source tables, SQL logic (CASE WHEN, SUM, AVG, etc.), and the file where it is defined.

You can also explore what feeds into a table:
- *"What are the sources of `silver.fact_jobs`?"* → calls `get_table_sources`
- *"Show me the full lineage tree for `conformed.churn_risk`"* → calls `get_lineage_tree`

---

### 🎓 Feature #2 — Senior Peer Review

**What it is & why it matters:**
Before committing SQL changes, Tracepipe AI reviews your git diff like a senior data engineer who knows your project. It detects SQL syntax errors, identifies which tables you changed and what exactly changed (from → to), traces the downstream impact chain through your real lineage, and flags grain mismatches — cases where a changed table produces data at a different granularity than the downstream table expects, which silently corrupts metrics.

**How to use it:**

**Step 1 — Run setup once** (first time only, builds business context from your ETL files):

Ask in chat: *"Run peer review setup"* — or the AI will call `peer_review_setup()` automatically.

**Step 2 — Make your SQL changes** as normal in your repo.

**Step 3 — Ask for a review** before committing:

- *"Peer review my changes"*
- *"Check my SQL before I commit"*
- *"What's the impact of my changes?"*

The AI calls `peer_review()` and returns a report like:

```
PEER REVIEW REPORT

SENIOR DATA ENGINEER NOTES:
  - You changed ETL script(s): etl/silver_layer.sql. These drive the pipeline; impact below.
  - silver.fact_jobs feeds 3 downstream table(s). Confirm metrics and grain stay aligned.

** silver.fact_jobs (file: etl/silver_layer.sql)
   Changed from: WHERE status = 'active'; GROUP BY
   To: AVG() changed to SUM(); WHERE clause modified
   This change impacts: conformed.company_stats, gold.executive_summary

BUSINESS (grain / logic mismatch):
  - Downstream table conformed.company_stats is built at company-level grain...

---
```

Risk levels: 🟢 GREEN (safe to commit) · 🟡 YELLOW (caution) · 🔴 RED (manual review required)

---

### 💥 Feature #3 — Impact Analysis

**What it is & why it matters:**
Before making any change, understand the full blast radius. You pick a table you are planning to modify and Tracepipe AI instantly shows you every downstream table that depends on it — directly and transitively — along with a risk level. For Databricks, this reads live from `system.access.table_lineage` (Unity Catalog). No JSON files, no setup, no commands.

**How to use it:**
Just ask in the chat:

- *"I want to change `silver.fact_jobs`, what will be impacted?"*
- *"What breaks downstream if I modify `raw.employees`?"*
- *"Show me the blast radius of `conformed.company_stats`"*

The AI calls `analyze_impact(table_name)` and returns a report like:

```
IMPACT ANALYSIS: silver.fact_jobs

Risk: YELLOW — 3 downstream tables affected — proceed with caution.

Downstream tables that will be affected:
  - conformed.company_stats   (direct dependency)
  - conformed.churn_risk      (direct dependency)
  - gold.executive_summary    (hop 2)

Total impacted: 3 table(s)
```

Risk levels: 🟢 GREEN (no downstream) · 🟡 YELLOW (1–4 tables) · 🔴 RED (5+ tables)

---

## 📅 2026-04-12

### 🔗 Feature #32 — Enhance Spark UDF and Complex Transformation Parsing

**What it is & why it matters:**
Improve the Spark analysis engine to reliably extract column-level lineage even through Python/Scala UDFs and complex DataFrame operations that currently limit native Unity Catalog lineage. This will make the lineage significantly more accurate and comprehensive for advanced Databricks users, addressing a critical gap where current solutions often fall short.

**How to use it:**

See `tests/test_spark_lineage.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/42)

---

## 📅 2026-04-19

### 🔗 Feature #41 — Automated Data Governance Policies & Anomaly Detection

**What it is & why it matters:**
Imagine you have a robot guard watching over your toy collection. You tell the guard the rules: 'My race car should always have 4 wheels' and 'My teddy bear should be brown.' This feature is like that guard for data - it watches tables and data in Databricks and alerts you when something breaks the rules you set, like when a column disappears or weird numbers show up. This matters because bad data can break important programs and reports, and catching problems early saves a lot of time and headaches.

**Tested on real data (companies_data catalog):**
```
Query ran OK but returned 0 rows.
SQL: SELECT table_name, column_name, data_type FROM companies_data.information_schema.columns WHERE table_schema = 'main' LIMIT 5
```

**How to use it:**

Step 1: Define governance policies in YAML or Python with schema and quality rules.
Step 2: Run the governance engine to monitor assets: `python -m scripts.governance_engine --catalog companies_data --scan`.
Step 3: Review detected anomalies and violations in the output JSON.
Step 4: Set up continuous monitoring by scheduling the engine to run periodically.

Example Python usage:
python
from scripts.governance_engine import GovernanceEngine
from scripts.peer_review.governance_policy import GovernancePolicy

engine = GovernanceEngine()
policy = GovernancePolicy(
    policy_id='schema_policy_1',
    name='Customer Table Schema',
    description='Enforce customer table schema',
    rules={'required_columns': 'id,name,email', 'max_null_rate': '0.05'},
    severity='high'
)
engine.add_policy(policy)
violations = engine.check_asset('companies_data.main.customers')
print(violations)


🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/51)
