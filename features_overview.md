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

### 🔗 Feature #31 — Proactive Lineage-Driven Impact Analysis & Data Reliability

**What it is & why it matters:**
Databricks Unity Catalog users experience challenges with lost lineage for renamed objects and inconsistencies where lineage information is reportedly overwritten or partially lost after subsequent job runs, hindering reliable impact analysis and debugging. This Tracepipe AI feature will introduce a robust 'Impact Analysis Dashboard' with versioned lineage, ensuring persistent tracking even for renamed objects. It will automatically detect upstream schema changes or data quality anomalies and leverage the complete lineage graph to proactively identify and alert all affected downstream data assets and stakeholders, transforming reactive troubleshooting into proactive data governance and improving overall data reliability.

**How to use it:**

See `tests/test_impact_analysis.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/41)
