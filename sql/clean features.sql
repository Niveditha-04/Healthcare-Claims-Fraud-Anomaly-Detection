TRUNCATE TABLE claims_clean;

WITH base AS (
  SELECT
    clm_id::bigint,
    desynpuf_id               AS bene_id,
    prf_physn_npi_1::bigint   AS prf_physn_npi_1,
    sp_state_code,
    bene_county_cd,
    TO_DATE(NULLIF(bene_birth_dt,'0'),'YYYYMMDD') AS bene_birth_dt,
    NULLIF(bene_death_dt,'0')::text AS bene_death_dt_text,
    TO_DATE(NULLIF(clm_from_dt,'0'),'YYYYMMDD')   AS clm_from_dt,
    TO_DATE(NULLIF(clm_thru_dt,'0'),'YYYYMMDD')   AS clm_thru_dt,
    bene_sex_ident_cd          AS sex,
    bene_race_cd               AS race,
    COALESCE(plan_cvrg_mos_num, bene_hi_cvrage_tot_mons) AS coverage_mos,
    sp_chrnkidn                AS chronic_ckd,
    icd9_dgns_cd_1,
    hcpcs_cd_1,
    line_nch_pmt_amt_1::numeric                AS line_nch_pmt_amt_1,
    COALESCE(line_bene_ptb_ddctbl_amt_1,0)::numeric
      + COALESCE(line_coinsrnc_amt_1,0)::numeric   AS pt_liability,
    COALESCE(medreimb_car, medreimb_op, medreimb_ip)::numeric AS total_reimb
  FROM claims_raw
  WHERE clm_id IS NOT NULL
)
, typed AS (
  SELECT *,
    CASE WHEN bene_death_dt_text IS NOT NULL AND bene_death_dt_text <> '0'
         THEN TO_DATE(bene_death_dt_text,'YYYYMMDD') END AS bene_death_dt
  FROM base
)
, enriched AS (
  SELECT
    clm_id, bene_id, prf_physn_npi_1, sp_state_code, bene_county_cd,
    sex, race,
    coverage_mos::int,
    chronic_ckd::int,
    clm_from_dt, clm_thru_dt,
    GREATEST(0, COALESCE((clm_thru_dt - clm_from_dt),0))::int AS los_days,
    icd9_dgns_cd_1, hcpcs_cd_1,
    line_nch_pmt_amt_1, pt_liability, total_reimb,
    CASE
      WHEN bene_birth_dt IS NOT NULL AND clm_from_dt IS NOT NULL
      THEN DATE_PART('year', AGE(clm_from_dt, bene_birth_dt))::int
    END AS age
  FROM typed
)
, provider_stats AS (
  SELECT
    prf_physn_npi_1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY line_nch_pmt_amt_1) AS provider_median,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY line_nch_pmt_amt_1) AS provider_p95
  FROM enriched
  WHERE line_nch_pmt_amt_1 IS NOT NULL
  GROUP BY 1
)
INSERT INTO claims_clean (
  clm_id, bene_id, prf_physn_npi_1, sp_state_code, bene_county_cd,
  age, sex, race, coverage_mos, chronic_ckd,
  clm_from_dt, clm_thru_dt, los_days, icd9_dgns_cd_1, hcpcs_cd_1,
  line_nch_pmt_amt_1, pt_liability, total_reimb,
  provider_median, provider_p95, amt_to_p95_ratio
)
SELECT
  e.clm_id, e.bene_id, e.prf_physn_npi_1, e.sp_state_code, e.bene_county_cd,
  e.age, e.sex, e.race, e.coverage_mos, e.chronic_ckd,
  e.clm_from_dt, e.clm_thru_dt, e.los_days, e.icd9_dgns_cd_1, e.hcpcs_cd_1,
  e.line_nch_pmt_amt_1, e.pt_liability, e.total_reimb,
  s.provider_median, s.provider_p95,
  CASE WHEN s.provider_p95 IS NOT NULL AND s.provider_p95 > 0
       THEN e.line_nch_pmt_amt_1 / s.provider_p95 END AS amt_to_p95_ratio
FROM enriched e
LEFT JOIN provider_stats s USING (prf_physn_npi_1);
