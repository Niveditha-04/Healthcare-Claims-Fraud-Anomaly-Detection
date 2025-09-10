from pathlib import Path
import pandas as pd

# This file lives in: <project_root>/src/peek.py
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Search both the project root and an optional "data" subfolder
SEARCH_DIRS = [PROJECT_ROOT / "data", PROJECT_ROOT]

# Prefer this filename if present, otherwise pick the first CSV/Parquet found
PREFERRED = "MedicalClaimsSynthetic1M.csv"

def find_dataset():
    # First look for the preferred file in both locations
    for d in SEARCH_DIRS:
        f = d / PREFERRED
        if f.exists():
            return f

    # Otherwise, grab the first CSV/Parquet we can find
    candidates = []
    for d in SEARCH_DIRS:
        candidates += list(d.glob("*.csv"))
        candidates += list(d.glob("*.parquet"))

    if not candidates:
        raise FileNotFoundError(
            "No CSV/Parquet files found in:\n  - "
            + "\n  - ".join(str(p) for p in SEARCH_DIRS)
            + "\nTip: put your CSV in the project root or in a 'data' folder."
        )
    return candidates[0]

def main():
    f = find_dataset()
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Reading file: {f}")

    if f.suffix.lower() == ".parquet":
        df = pd.read_parquet(f)
    else:
        # Read a small sample fast
        df = pd.read_csv(f, nrows=5000)

    print("Rows read:", len(df))
    print("Columns:", list(df.columns))
    print(df.head(5))

if __name__ == "__main__":
    main()
