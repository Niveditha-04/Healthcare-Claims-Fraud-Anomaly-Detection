**Tools:** Python, PostgreSQL, SQL, Power BI  
**Dataset:** [Medical Claims Synthetic 1M (Kaggle)](https://www.kaggle.com/datasets/drscarlat/medicalclaimssynthetic1m)  

---

## Why this project
Healthcare claims often involve millions of records, and fraudulent billing can cost providers and insurers billions every year. The idea here is to simulate how a data team would:  
- Load raw claims data into a database,  
- Clean and standardize the data,  
- Apply rule-based checks and statistical outlier detection, and  
- Present results in a dashboard.  

---

## Process
- **Data Ingestion:** Loaded CSV into PostgreSQL using Python (pandas + psycopg2).
- **Database schema:** A “peek” script was created to inspect the dataset and verify the schema before loading.
- **Cleaning & feature engineering:** Derived features (patient age, coverage months, length of stay, payment ratios).  
- **Fraud detection rules:** Duplicate claims, coverage violations, diagnosis–procedure mismatches, upcoding, liability issues.  
- **Provider Risk:** Outlier scoring for providers based on claim patterns (Number of claims submitted, Median payments, Rate of flagged claims, Variety of procedures billed).
- **Anomaly Scoring:** Combined rule flags + payment residuals + provider risk.  
- **Priority Selection:** Top 1% of claims (~128k) flagged for review.  
- **Dashboard (Power BI):** KPIs, top providers, state trends, priority queue.  


### Database schema
Separate tables were set up:
- `claims_raw` for the unprocessed data,  
- `claims_clean` for cleaned and enriched features,  
- `rule_flags` for rule-based fraud checks,  
- `provider_risk` for provider-level outlier scoring,  
- `claims_scored` for combined anomaly scores.  

"This mirrors a production-style ETL pipeline."

---

## Results
- 1M+ claims processed in PostgreSQL.  
- ~3% of claims flagged by at least one fraud rule.  
- Top 1% prioritized (~128k claims) for manual review.  
- Provider scoring highlights high-risk NPIs whose flagged rates are above 90%.

---

## Benefits
Shows how SQL + Python pipelines can detect anomalies in large healthcare datasets and how dashboards turn outputs into actionable insights.
