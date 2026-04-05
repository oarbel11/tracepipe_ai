# Tracepipe AI — Features Overview

This file lists features that are **confirmed working** (all tests passed).
Each entry explains what the feature does and how to use it.

---

---

## 📅 2026-04-05

### 🔗 Feature #12 — Semantic Layer & BI Tool Integration

**What it is & why it matters:**
Business users often begin their data journey from a BI dashboard or report, needing to trace metrics back to their source. This feature will integrate with popular BI platforms (e.g., Power BI, Tableau, Looker) to pull metadata and link dashboard elements, measures, and reports directly to the underlying Unity Catalog tables and columns, providing a complete journey from a business metric back to raw data.

**How to use it:**

See `tests/test_bi_integration.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/34)
