# Tracepipe AI — Features Overview

This file lists features that are **confirmed working** (all tests passed).
Each entry explains what the feature does and how to use it.

---

---

## 📅 2026-04-05

### 🔗 Feature #13 — Code-Level Transformation Tracing

**What it is & why it matters:**
Databricks' Unity Catalog provides high-level table lineage, but users struggle to understand the 'how' behind data transformations within notebooks, dbt models, or complex Spark SQL. This feature will parse and visualize granular column-level transformations, UDFs, and intermediate steps directly within Tracepipe's lineage graph, providing unprecedented visibility into data flow logic.

**How to use it:**

See `tests/test_transformation_tracer.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/35)
