TRUNCATE TABLE claims_scored;

WITH base AS (
  SELECT
    c.clm_id,
    c.prf_physn_npi_1,
    c.line_nch_pmt_amt_1,
    c.provider_p95,
    r.rule_count,
    p.risk_score
  FROM claims_clean c
  LEFT JOIN rule_flags r USING (clm_id)
  LEFT JOIN provider_risk p ON c.prf_physn_npi_1 = p.prf_physn_npi_1
),
residuals AS (
  SELECT
    b.*,
    
    GREATEST(0, b.line_nch_pmt_amt_1 - COALESCE(b.provider_p95,0)) / NULLIF(b.provider_p95,0) AS resid_ratio
  FROM base b
),
scaled AS (
  SELECT
    r.*,
    
    (r.resid_ratio - MIN(r.resid_ratio) OVER ())
      / NULLIF(MAX(r.resid_ratio) OVER () - MIN(r.resid_ratio) OVER (),0)     AS s_resid,
    
    LEAST(COALESCE(r.rule_count,0),5) / 5.0                                   AS s_rules,
    
    COALESCE(r.risk_score,0) / 100.0                                          AS s_prisk
  FROM residuals r
),
scored AS (
  SELECT
    clm_id,
    
    (0.50*COALESCE(s_resid,0) + 0.35*COALESCE(s_rules,0) + 0.15*COALESCE(s_prisk,0)) AS anomaly_score,
    COALESCE(rule_count,0) AS rule_count
  FROM scaled
)
INSERT INTO claims_scored (clm_id, anomaly_score, rule_count, is_priority)
SELECT
  clm_id,
  anomaly_score,
  rule_count,
  (anomaly_score >= 0.85) AS is_priority
FROM scored;
