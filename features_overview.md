# Tracepipe AI — Features Overview

This file lists features that are **confirmed working** (all tests passed).
Each entry explains what the feature does and how to use it.

---

---

## 📅 2026-04-05

### 🔗 Feature #16 — Lineage-Driven Data Observability & Governance Alerts

**What it is & why it matters:**
Databricks users need proactive monitoring of data health and automated enforcement of governance policies, which current tools lack in direct lineage integration. This feature will allow users to define data quality rules or governance policies on specific assets; upon violation (e.g., schema change, freshness anomaly, PII detection), Tracepipe will leverage its lineage graph to identify all impacted downstream assets and automatically alert relevant data owners and stakeholders.

**How to use it:**

See `tests/test_governance.py` for usage examples.

🔗 [View PR](https://github.com/oarbel11/tracepipe_ai/pull/37)
