#!/usr/bin/env python3
"""
load_iapd_to_postgres.py – robust bulk loader for SEC IAPD **Registered Adviser**
files (Excel & CSV). Skips ERA (Exempt Reporting Adviser) files to avoid schema
mismatches and speed the run.

Key fixes v1.4
--------------
* **.env support out‑of‑the‑box** – automatically loads environment variables
  from a `.env` file if present (via `python‑dotenv`).
* Retains all v1.3 features: header coercion, resource‑fork skip, parallel
  ingestion, default exclusion of ERA files, faster chunksize.

Usage
-----
    # env vars from shell OR .env will be picked up automatically
    python3 load_iapd_to_postgres.py data/unzipped/iapd --workers 4
"""
from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List

import pandas as pd
import sqlalchemy as sa
from tqdm import tqdm

# --------------------- NEW: auto‑load .env if present --------------------- #
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except ModuleNotFoundError:
    # python‑dotenv not installed – silently continue; env vars must be in shell
    pass

# -------------------------- CONFIG & HEADER MAPS -------------------------- #
HEADER_VARIANTS = {
    "crd": ["FirmCrdNb"],
    "filing_date": ["FilingDate"],
    "raum": ["RAUM_5B2", "RegulatoryAssetsUnderManagement", "Total_RAUM"],
    "total_accounts": ["Item5F2f_TotalAccts", "TotalNumberOfAccounts"],
    "disclosure_flag": ["Item11DisclosureFlag", "HasDisciplinaryHistory"],
}
CLIENT_BUCKET_PREFIX = "Item5D_1"  # Item 5.D(1)(A‑J)

CREATE_SQL = """
create table if not exists ia_filing (
  crd             int,
  filing_date     date,
  raum            numeric,
  total_clients   int,
  total_accounts  int,
  cco_id          text,
  disclosure_flag char(1),
  primary key (crd, filing_date)
);
"""

# ------------------------------------------------------------------------- #

def get_engine() -> sa.Engine:
    for v in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        if v not in os.environ:
            sys.exit(f"{v} not set (define in shell or .env)")
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT", "5432")
    creds = f"{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
    url = f"postgresql+psycopg2://{creds}@{host}:{port}/{os.environ['PGDATABASE']}"
    return sa.create_engine(url, pool_pre_ping=True)


# ------------------------------- HELPERS ---------------------------------- #


def read_any(path: Path) -> pd.DataFrame:
    """Return DataFrame (all str) for .xlsx/.xls/.csv; skip macOS stubs."""
    if path.name.startswith("._"):
        raise RuntimeError("resource‑fork stub – skip")
    ext = path.suffix.lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    if ext == ".csv":
        return pd.read_csv(path, dtype=str, sep=",|\|", engine="python")
    raise ValueError(f"Unsupported extension: {path}")


def pick_column(df: pd.DataFrame, variants: List[str]) -> str | None:
    for c in variants:
        if c in df.columns:
            return c
    return None


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.map(str)  # ensure headers are str

    out = pd.DataFrame()
    for canonical, variants in HEADER_VARIANTS.items():
        src = pick_column(df, variants)
        out[canonical] = df[src] if src else None

    # numeric casts
    for col in ("raum", "total_accounts"):
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # total_clients
    client_cols = [c for c in df.columns if c.startswith(CLIENT_BUCKET_PREFIX)]
    if client_cols:
        out["total_clients"] = df[client_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
    else:
        out["total_clients"] = None

    # composite cco_id
    if {
        "ChiefComplianceOfficer_FirstName",
        "ChiefComplianceOfficer_LastName",
        "ChiefComplianceOfficer_CRD",
    }.issubset(df.columns):
        out["cco_id"] = (
            df["ChiefComplianceOfficer_FirstName"].fillna("").str.strip().str.upper()
            + "|"
            + df["ChiefComplianceOfficer_LastName"].fillna("").str.strip().str.upper()
            + "|"
            + df["ChiefComplianceOfficer_CRD"].fillna("").astype(str)
        )
    else:
        out["cco_id"] = None

    return out


def ingest_file(path: Path, dsn: str):
    try:
        engine = sa.create_engine(dsn, pool_pre_ping=True)
        raw = read_any(path)
        df = normalise(raw)
        df.to_sql("ia_filing", engine, if_exists="append", index=False, method="multi", chunksize=5000)
        return f"✓ {path.name}: {len(df)} rows"
    except RuntimeError as skip:
        return f"· {path.name}: {skip}"
    except Exception as exc:
        return f"✗ {path.name}: {exc}"


# --------------------------------- MAIN ----------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Load SEC IAPD Registered IA files into Postgres")
    parser.add_argument("src_dir", help="Directory with extracted files")
    parser.add_argument("--workers", type=int, default=1, help="Parallel processes (default 1)")
    parser.add_argument("--include-exempt", action="store_true", help="Also ingest ERA 'exempt' files")
    args = parser.parse_args()

    src = Path(args.src_dir).expanduser().resolve()
    if not src.exists():
        sys.exit(f"Source dir not found: {src}")

    patterns = ["*.xlsx", "*.xls", "*.csv"]
    files = [p for pat in patterns for p in src.rglob(pat)]

    # filter exempt unless requested
    if not args.include_exempt:
        files = [f for f in files if "exempt" not in f.name.lower()]

    if not files:
        sys.exit("No data files found after filtering")

    dsn = str(get_engine().url)
    print(f"Ingesting {len(files)} files → ia_filing using {args.workers} worker(s)\n")

    if args.workers == 1:
        for f in tqdm(files, unit="file"):
            print(ingest_file(f, dsn))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(ingest_file, f, dsn): f for f in files}
            for fut in tqdm(as_completed(futures), total=len(files), unit="file"):
                print(fut.result())

    print("\nDone ✔ – check ia_filing for rows.")


if __name__ == "__main__":
    main()
