Here is the corrected README — replace what you have with this:

<div align="center">

# Healthcare Claims Fraud & Anomaly Detection

**End-to-end SQL + Python pipeline that scores 1 million+ insurance claims for fraud risk and surfaces a priority review queue through a Power BI dashboard.**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=flat)](LICENSE)
[![Dataset](https://img.shields.io/badge/Dataset-Kaggle%201M%20Claims-20BEFF?style=flat&logo=kaggle&logoColor=white)](https://www.kaggle.com/datasets/drscarlat/medicalclaimssynthetic1m)

</div>

---

## The Problem

Healthcare fraud costs the U.S. an estimated **$300 billion per year** — roughly 10 cents of every dollar spent on care. Insurers and providers receive millions of claims monthly; manual review can cover only a fraction. This project simulates how a data engineering team would build a scalable, automated fraud-scoring pipeline that:

1. Ingests raw claims at scale into a relational database
2. Applies rule-based fraud checks grounded in real billing regulations
3. Scores every claim with a composite anomaly model
4. Surfaces the highest-risk cases in an analyst-ready dashboard

---

## Approach

### Pipeline — 5 Stages

| Stage | Script | What happens |
|-------|--------|--------------|
| 1. Ingest | `src/ingest.py` | CSV loaded into PostgreSQL in 100k-row chunks via pandas + SQLAlchemy |
| 2. Clean & Enrich | `sql/clean features.sql` | Derives age, length-of-stay, coverage months, payment ratios, provider P50/P95 |
| 3. Fraud Rules | `sql/rule flags.sql` | 5 rule-based flags written per claim into `rule_flags` |
| 4. Provider Risk | `sql/provider risk.sql` | NPI-level composite risk score (0–100) into `provider_risk` |
| 5. Score & Prioritize | `sql/claims scored.sql` + `sql/calibrate priority.sql` | Composite anomaly score per claim; top 1% marked `is_priority = TRUE` |

### Fraud Detection Rules

| # | Rule | Logic |
|---|------|-------|
| 1 | **Duplicate / Overlapping Claims** | Same beneficiary + diagnosis with overlapping claim dates (LAG window function) |
| 2 | **Coverage Violation** | Claim submitted with zero or missing coverage months |
| 3 | **Diagnosis–Procedure Mismatch** | Dialysis HCPCS codes billed for patients with no chronic kidney disease diagnosis |
| 4 | **Upcoding** | Payment exceeds Q3 + 3×IQR for that HCPCS code — statistical outlier per procedure |
| 5 | **Liability Anomaly** | Patient cost-share ratio outside the expected 2%–50% range |

### Provider Risk Score (0–100)

Each NPI is scored using three normalized signals:

| Signal | Weight |
|--------|--------|
| Scaled median payment amount | 40% |
| Scaled rate of flagged claims | 40% |
| Scaled variety of procedures billed | 20% |

### Composite Anomaly Score (per claim)

Each claim gets a single score blending all pipeline outputs:

| Component | Weight | Meaning |
|-----------|--------|---------|
| Payment residual ratio | 50% | How far the claim sits above that provider's 95th-percentile payment |
| Rule score | 35% | Number of fraud rules triggered (0–5), normalized |
| Provider risk score | 15% | NPI-level risk, normalized 0–1 |

Claims scoring in the **top 1% (≥ 99th percentile)** are marked `is_priority = TRUE`.

---

## Results

| Metric | Value |
|--------|-------|
| Claims processed | **1,000,000+** |
| Fraud rules applied | **5** |
| Claims flagged by ≥ 1 rule | **~30,000 (~3%)** |
| Priority review queue (top 1%) | **~128,000 claims** |
| High-risk NPIs identified (flag rate > 90%) | Surfaced in provider risk table |
| ETL tables in pipeline | **5** |
| Ingest chunk size | **100,000 rows / chunk** |

---

## Demo

### Video Walkthrough
> *2-minute walkthrough: pipeline architecture → SQL logic → Power BI dashboard outputs.*

[![Watch the Demo](https://img.shields.io/badge/Watch%20Demo-Coming%20Soon-red?style=for-the-badge&logo=youtube)](#)

### Dashboard Preview (Power BI)

| View | What it shows |
|------|---------------|
| KPI Page | Total claims · flagged % · priority count · avg anomaly score |
| Provider Risk | Top 20 NPIs ranked by risk score with flag rate breakdown |
| State Trends | Fraud flag density by state |
| Priority Queue | Filterable table of ~128k priority claims for analyst review |

*Dashboard screenshots will be added here.*

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Data ingestion | Python · pandas · SQLAlchemy · psycopg2 |
| Storage & transformation | PostgreSQL |
| Fraud rules & scoring | SQL window functions · CTEs · percentile aggregates |
| Visualization | Power BI |
| Dataset | [Medical Claims Synthetic 1M — Kaggle](https://www.kaggle.com/datasets/drscarlat/medicalclaimssynthetic1m) |

---

## Database Schema

| Table | Purpose |
|-------|---------|
| `claims_raw` | Unprocessed data as loaded from CSV |
| `claims_clean` | Cleaned features: age, LOS, payment ratios, provider percentiles |
| `rule_flags` | Boolean per fraud rule + total `rule_count` per claim |
| `provider_risk` | NPI-level composite risk score |
| `claims_scored` | Final anomaly score + `is_priority` flag per claim |

