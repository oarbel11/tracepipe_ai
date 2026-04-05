# Tracepipe AI — Features Overview

This file lists features that are **confirmed working** (all tests passed).
Each entry explains what the feature does and how to use it.

---

---

## 📅 2026-04-05

### 🔗 Feature #3 — Interactive Impact Analysis & Governance Policy Overlay

**What it is & why it matters:**
Understanding the full 'blast radius' of a schema change, data quality incident, or compliance policy update is challenging with static lineage views, leading to errors and delays. Tracepipe AI will provide an interactive UI for impact analysis, allowing users to select an asset and instantly visualize all dependent downstream assets, filtered by tags (e.g., PII), ownership, or data quality status, while also overlaying relevant governance policies directly onto the lineage graph.

**How to use it:**

See `tests/test_impact_analysis.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/31)
