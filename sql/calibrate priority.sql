WITH cutoff AS (
  SELECT
    percentile_cont(0.99) WITHIN GROUP (ORDER BY anomaly_score) AS p_cut
  FROM claims_scored
)
UPDATE claims_scored s
SET is_priority = (s.anomaly_score >= c.p_cut)
FROM cutoff c;
