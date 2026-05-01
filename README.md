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

### Pipeline Architecture
CSV (1M+ rows)
│
▼
┌─────────────┐ ingest.py (pandas + SQLAlchemy)
│ claims_raw │◄────────────────────────────────────
└─────────────┘
│
▼
┌──────────────┐ clean features.sql
│ claims_clean │◄── age · LOS · coverage · payment ratios · provider percentiles
└──────────────┘
│
├──────────────────────────────────────────────┐
▼ ▼
┌────────────┐ rule flags.sql ┌───────────────┐ provider risk.sql
│ rule_flags │ 5 fraud rules │ provider_risk │ composite NPI score
└────────────┘ └───────────────┘
│ │
└───────────────────┬──────────────────────────┘
▼
┌────────────────┐ claims scored.sql
│ claims_scored │ anomaly_score = 0.50·residual
└────────────────┘ + 0.35·rules
│ + 0.15·provider_risk
▼
┌────────────────┐ calibrate priority.sql
│ Priority Queue│ top 1% by anomaly_score → is_priority = TRUE
└────────────────┘
│
▼
Power BI Dashboard

### Fraud Detection Rules
| # | Rule | Logic |
|---|------|-------|
| 1 | **Duplicate / Overlapping Claims** | Same beneficiary + diagnosis with overlapping claim dates (LAG window function) |
| 2 | **Coverage Violation** | Claim submitted with zero or missing coverage months |
| 3 | **Diagnosis–Procedure Mismatch** | Dialysis HCPCS codes billed for patients with no chronic kidney disease diagnosis |
| 4 | **Upcoding** | Payment exceeds Q3 + 3×IQR for that HCPCS code — statistical outlier per procedure |
| 5 | **Liability Anomaly** | Patient cost-share ratio outside the expected 2%–50% range |
### Provider Risk Scoring
Each NPI receives a composite risk score (0–100):
risk_score = 100 × (0.40 × scaled_median_payment
+ 0.40 × scaled_flag_rate
+ 0.20 × scaled_procedure_variety)

Signals used: total claims submitted, median payment amount, rate of flagged claims, variety of procedures billed.
### Composite Anomaly Score
Each claim gets a single score combining all pipeline signals:
anomaly_score = 0.50 × payment_residual_ratio (how far above provider's P95)
+ 0.35 × rule_score (0–5 rules triggered, normalized)
+ 0.15 × provider_risk_score (normalized 0–1)

Claims at or above the **99th percentile** are marked `is_priority = TRUE`.
---
## Results
| Metric | Value |
|--------|-------|
| Claims processed | **1,000,000+** |
| Fraud rules applied | **5** |
| Claims flagged by ≥ 1 rule | **~30,000 (~3%)** |
| Priority review queue (top 1%) | **~128,000 claims** |
| Anomaly score threshold for priority | **≥ 0.85** |
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
| State Trends | Fraud flag density by state code |
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
Five tables form the production-style ETL pipeline:
| Table | Purpose |
|-------|---------|
| `claims_raw` | Unprocessed data as loaded from CSV |
| `claims_clean` | Cleaned and enriched features (age, LOS, payment ratios, provider percentiles) |
| `rule_flags` | One row per claim with a boolean per fraud rule and a total `rule_count` |
| `provider_risk` | NPI-level composite risk score |
| `claims_scored` | Final anomaly score + `is_priority` flag per claim |
