TRUNCATE TABLE rule_flags;

WITH base AS (
  SELECT
    clm_id, bene_id, icd9_dgns_cd_1, hcpcs_cd_1,
    clm_from_dt, clm_thru_dt,
    coverage_mos, chronic_ckd,
    line_nch_pmt_amt_1,
    pt_liability
  FROM claims_clean
),

-- Rule 1: find overlaps within (bene_id, dx), comparing to previous claim
ordered AS (
  SELECT
    b.*,
    LAG(clm_thru_dt) OVER (PARTITION BY bene_id, icd9_dgns_cd_1 ORDER BY clm_from_dt, clm_thru_dt) AS prev_thru
  FROM base b
),
r1 AS (
  SELECT
    clm_id,
    CASE WHEN prev_thru IS NOT NULL AND clm_from_dt <= prev_thru THEN TRUE ELSE FALSE END AS rule_dup
  FROM ordered
),

-- Rule 2: coverage <= 0 → violation
r2 AS (
  SELECT clm_id, (COALESCE(coverage_mos,0) <= 0) AS rule_cov
  FROM base
),

-- Rule 3: toy mismatch — dialysis-like HCPCS codes vs no CKD
r3 AS (
  SELECT clm_id,
         ( (hcpcs_cd_1 ILIKE 'DIA%' OR hcpcs_cd_1 IN ('90935','90937','90945','90947'))  -- dialysis-ish
           AND COALESCE(chronic_ckd,0) <> 1 ) AS rule_mismatch
  FROM base
),

-- Rule 4: upcoding — Q3 + 3*IQR by HCPCS
hcpcs_stats AS (
  SELECT
    hcpcs_cd_1,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY line_nch_pmt_amt_1) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY line_nch_pmt_amt_1) AS q3
  FROM base
  WHERE line_nch_pmt_amt_1 IS NOT NULL
  GROUP BY 1
),
r4 AS (
  SELECT b.clm_id,
         CASE
           WHEN s.q1 IS NULL OR s.q3 IS NULL THEN FALSE
           WHEN (s.q3 - s.q1) IS NULL OR (s.q3 - s.q1) = 0 THEN FALSE
           ELSE (b.line_nch_pmt_amt_1 > s.q3 + 3*(s.q3 - s.q1))
         END AS rule_upcode
  FROM base b
  LEFT JOIN hcpcs_stats s USING (hcpcs_cd_1)
),

-- Rule 5: liability anomaly — ratio outside [2%, 50%]
r5 AS (
  SELECT clm_id,
         CASE
           WHEN line_nch_pmt_amt_1 IS NULL OR line_nch_pmt_amt_1 = 0 THEN FALSE
           ELSE (
             (COALESCE(pt_liability,0) / NULLIF(line_nch_pmt_amt_1,0)) < 0.02
             OR
             (COALESCE(pt_liability,0) / NULLIF(line_nch_pmt_amt_1,0)) > 0.50
           )
         END AS rule_liability
  FROM base
)

INSERT INTO rule_flags (clm_id, rule_dup, rule_cov, rule_mismatch, rule_upcode, rule_liability, rule_count)
SELECT
  b.clm_id,
  COALESCE(r1.rule_dup, FALSE)         AS rule_dup,
  COALESCE(r2.rule_cov, FALSE)         AS rule_cov,
  COALESCE(r3.rule_mismatch, FALSE)    AS rule_mismatch,
  COALESCE(r4.rule_upcode, FALSE)      AS rule_upcode,
  COALESCE(r5.rule_liability, FALSE)   AS rule_liability,
 
  (CASE WHEN COALESCE(r1.rule_dup,FALSE) THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(r2.rule_cov,FALSE) THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(r3.rule_mismatch,FALSE) THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(r4.rule_upcode,FALSE) THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(r5.rule_liability,FALSE) THEN 1 ELSE 0 END) AS rule_count
FROM base b
LEFT JOIN r1 USING (clm_id)
LEFT JOIN r2 USING (clm_id)
LEFT JOIN r3 USING (clm_id)
LEFT JOIN r4 USING (clm_id)
LEFT JOIN r5 USING (clm_id);

-- helpful indexes 
CREATE INDEX IF NOT EXISTS idx_rule_flags_clm ON rule_flags(clm_id);
CREATE INDEX IF NOT EXISTS idx_rule_flags_count ON rule_flags(rule_count);
