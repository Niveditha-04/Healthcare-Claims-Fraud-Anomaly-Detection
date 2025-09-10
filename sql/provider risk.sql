TRUNCATE TABLE provider_risk;

WITH feat AS (
  SELECT
    c.prf_physn_npi_1,
    COUNT(*)                                      AS claims_cnt,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY c.line_nch_pmt_amt_1) AS median_payment,
    AVG(CASE WHEN r.rule_count > 0 THEN 1.0 ELSE 0.0 END)              AS flag_rate,
    COUNT(DISTINCT c.hcpcs_cd_1)                 AS unique_hcpcs_cnt
  FROM claims_clean c
  LEFT JOIN rule_flags r USING (clm_id)
  GROUP BY c.prf_physn_npi_1
),
scaled AS (
  SELECT
    f.*,
    
    (f.median_payment - MIN(f.median_payment) OVER ())
      / NULLIF(MAX(f.median_payment) OVER () - MIN(f.median_payment) OVER (),0) AS s_median,
    (f.flag_rate - MIN(f.flag_rate) OVER ())
      / NULLIF(MAX(f.flag_rate) OVER () - MIN(f.flag_rate) OVER (),0) AS s_flagrate,
    (f.unique_hcpcs_cnt - MIN(f.unique_hcpcs_cnt) OVER ())
      / NULLIF(MAX(f.unique_hcpcs_cnt) OVER () - MIN(f.unique_hcpcs_cnt) OVER (),0) AS s_unique
  FROM feat f
)
INSERT INTO provider_risk (prf_physn_npi_1, claims_cnt, median_payment, flag_rate, unique_hcpcs_cnt, risk_score)
SELECT
  prf_physn_npi_1, claims_cnt, median_payment, flag_rate, unique_hcpcs_cnt,
  
  100.0 * (0.4*COALESCE(s_median,0) + 0.4*COALESCE(s_flagrate,0) + 0.2*COALESCE(s_unique,0)) AS risk_score
FROM scaled;
