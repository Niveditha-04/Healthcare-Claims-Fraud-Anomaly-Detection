-- raw table ---
CREATE TABLE IF NOT EXISTS claims_raw (
  DESYNPUF_ID              TEXT,
  BENE_BIRTH_DT            TEXT,
  BENE_DEATH_DT            TEXT,
  BENE_SEX_IDENT_CD        TEXT,
  BENE_RACE_CD             TEXT,
  BENE_ESRD_IND            TEXT,
  SP_STATE_CODE            TEXT,
  BENE_COUNTY_CD           TEXT,
  BENE_HI_CVRAGE_TOT_MONS  INT,
  BENE_SMI_CVRAGE_TOT_MONS INT,
  BENE_HMO_CVRAGE_TOT_MONS INT,
  PLAN_CVRG_MOS_NUM        INT,
  SP_ALZHDMTA              INT,
  SP_CHF                   INT,
  SP_CHRNKIDN              INT,
  SP_CNCR                  INT,
  SP_COPD                  INT,
  SP_DEPRESSN              INT,
  SP_DIABETES              INT,
  SP_ISCHMCHT              INT,
  SP_OSTEOPRS              INT,
  SP_RA_OA                 INT,
  SP_STRKETIA              INT,

  MEDREIMB_IP              NUMERIC,
  BENRES_IP                NUMERIC,
  PPPYMT_IP                NUMERIC,
  MEDREIMB_OP              NUMERIC,
  BENRES_OP                NUMERIC,
  PPPYMT_OP                NUMERIC,
  MEDREIMB_CAR             NUMERIC,
  BENRES_CAR               NUMERIC,
  PPPYMT_CAR               NUMERIC,

  CLM_ID                   BIGINT,
  CLM_FROM_DT              TEXT,   
  CLM_THRU_DT              TEXT,
  ICD9_DGNS_CD_1           TEXT,
  PRF_PHYSN_NPI_1          BIGINT,
  HCPCS_CD_1               TEXT,
  LINE_NCH_PMT_AMT_1       NUMERIC,
  LINE_BENE_PTB_DDCTBL_AMT_1 NUMERIC,
  LINE_COINSRNC_AMT_1      NUMERIC,
  LINE_PRCSG_IND_CD_1      TEXT,
  LINE_ICD9_DGNS_CD_1      TEXT
);

-- cleaned/enriched table
CREATE TABLE IF NOT EXISTS claims_clean (
  clm_id BIGINT PRIMARY KEY,
  bene_id TEXT,
  prf_physn_npi_1 BIGINT,
  sp_state_code TEXT,
  bene_county_cd TEXT,
  age INT,
  sex TEXT,
  race TEXT,
  coverage_mos INT,
  chronic_ckd INT,
  clm_from_dt DATE,
  clm_thru_dt DATE,
  los_days INT,
  icd9_dgns_cd_1 TEXT,
  hcpcs_cd_1 TEXT,
  line_nch_pmt_amt_1 NUMERIC,
  pt_liability NUMERIC,
  total_reimb NUMERIC,
  provider_median NUMERIC,
  provider_p95 NUMERIC,
  amt_to_p95_ratio NUMERIC
);

-- rule flags
CREATE TABLE IF NOT EXISTS rule_flags (
  clm_id BIGINT PRIMARY KEY,
  rule_dup BOOLEAN,
  rule_cov BOOLEAN,
  rule_mismatch BOOLEAN,
  rule_upcode BOOLEAN,
  rule_liability BOOLEAN,
  rule_count INT
);

-- provider risk
CREATE TABLE IF NOT EXISTS provider_risk (
  prf_physn_npi_1 BIGINT PRIMARY KEY,
  claims_cnt INT,
  median_payment NUMERIC,
  flag_rate NUMERIC,
  unique_hcpcs_cnt INT,
  risk_score NUMERIC
);

-- line-level anomaly score
CREATE TABLE IF NOT EXISTS claims_scored (
  clm_id BIGINT PRIMARY KEY,
  anomaly_score NUMERIC,
  rule_count INT,
  is_priority BOOLEAN
);

-- helpful indexes
CREATE INDEX IF NOT EXISTS idx_raw_provider ON claims_raw(prf_physn_npi_1);
CREATE INDEX IF NOT EXISTS idx_raw_bene     ON claims_raw(DESYNPUF_ID);
CREATE INDEX IF NOT EXISTS idx_raw_dates    ON claims_raw(CLM_FROM_DT, CLM_THRU_DT);
CREATE INDEX IF NOT EXISTS idx_raw_codes    ON claims_raw(HCPCS_CD_1, ICD9_DGNS_CD_1);
