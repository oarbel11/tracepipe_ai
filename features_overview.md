# Tracepipe AI — Features Overview

This file lists features that are **confirmed working** (all tests passed).
Each entry explains what the feature does and how to use it.

---

---

## 📅 2026-04-05

### 🔗 Feature #10 — Operational Lineage for Databricks Workloads

**What it is & why it matters:**
Unity Catalog provides table-level lineage, but often lacks visibility into the specific notebooks, Spark jobs, Delta Live Tables pipelines, or dbt models that generate or transform data. This feature will automatically capture and visualize operational lineage, linking code assets directly to the data assets they produce or consume, improving debugging, auditing, and understanding of data transformations.

**How to use it:**

See `tests/test_operational_lineage.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/32)
