from __future__ import annotations
from pathlib import Path
import os, sys, time
import pandas as pd
from sqlalchemy import create_engine, text

host = os.getenv("PG_HOST", "localhost")
port = os.getenv("PG_PORT", "5432")
db   = os.getenv("PG_DB", "claimsdb")
user = os.getenv("PG_USER", os.getenv("USER", "postgres"))
pwd  = os.getenv("PG_PASSWORD", "")
engine = create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}", future=True)

RAW_TABLE = "claims_raw"

ROOT = Path(__file__).resolve().parents[1]
PREF = ROOT / "MedicalClaimsSynthetic1M.csv"
ALT  = ROOT / "data" / "MedicalClaimsSynthetic1M.csv"
csv_path = PREF if PREF.exists() else ALT
if not csv_path.exists():
    sys.exit("❌ CSV not found at project root or ./data")

print(f"Loading CSV: {csv_path}")

RAW_COLS = [
  "desynpuf_id","bene_birth_dt","bene_death_dt","bene_sex_ident_cd","bene_race_cd","bene_esrd_ind",
  "sp_state_code","bene_county_cd","bene_hi_cvrage_tot_mons","bene_smi_cvrage_tot_mons","bene_hmo_cvrage_tot_mons",
  "plan_cvrg_mos_num","sp_alzhdmta","sp_chf","sp_chrnkidn","sp_cncr","sp_copd","sp_depressn","sp_diabetes",
  "sp_ischmcht","sp_osteoprs","sp_ra_oa","sp_strketia",
  "medreimb_ip","benres_ip","pppymt_ip","medreimb_op","benres_op","pppymt_op",
  "medreimb_car","benres_car","pppymt_car",
  "clm_id","clm_from_dt","clm_thru_dt","icd9_dgns_cd_1","prf_physn_npi_1","hcpcs_cd_1",
  "line_nch_pmt_amt_1","line_bene_ptb_ddctbl_amt_1","line_coinsrnc_amt_1","line_prcsg_ind_cd_1","line_icd9_dgns_cd_1"
]

INT_COLS = {
  "bene_hi_cvrage_tot_mons","bene_smi_cvrage_tot_mons","bene_hmo_cvrage_tot_mons","plan_cvrg_mos_num",
  "sp_alzhdmta","sp_chf","sp_chrnkidn","sp_cncr","sp_copd","sp_depressn","sp_diabetes","sp_ischmcht",
  "sp_osteoprs","sp_ra_oa","sp_strketia","clm_id","prf_physn_npi_1"
}
NUM_COLS = {
  "medreimb_ip","benres_ip","pppymt_ip","medreimb_op","benres_op","pppymt_op",
  "medreimb_car","benres_car","pppymt_car","line_nch_pmt_amt_1",
  "line_bene_ptb_ddctbl_amt_1","line_coinsrnc_amt_1"
}
DATE_COLS = {"clm_from_dt","clm_thru_dt","bene_birth_dt","bene_death_dt"}  

with engine.begin() as con:
    con.execute(text(f"TRUNCATE TABLE {RAW_TABLE};"))
print("Truncated claims_raw")

CHUNK = 100_000
total = 0
t0 = time.time()

for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK, dtype=str), start=1):
    chunk.columns = [c.lower() for c in chunk.columns]

    missing = [c for c in RAW_COLS if c not in chunk.columns]
    if missing:
        for m in missing:
            chunk[m] = pd.NA
    chunk = chunk[RAW_COLS]

    for c in chunk.columns:
        if c in INT_COLS:
            chunk[c] = pd.to_numeric(chunk[c], errors="coerce").astype("Int64")
        elif c in NUM_COLS:
            chunk[c] = pd.to_numeric(chunk[c], errors="coerce")
        elif c in DATE_COLS:
            chunk[c] = chunk[c].astype(str) 

    chunk.to_sql(RAW_TABLE, engine, if_exists="append", index=False, method="multi", chunksize=10_000)
    total += len(chunk)
    print(f"Chunk {i}: inserted {len(chunk):,} (total {total:,})")

print(f"Ingest complete — {total:,} rows in {time.time()-t0:,.1f}s")
