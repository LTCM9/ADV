#!/usr/bin/env python3
"""
load_iapd_to_postgres.py – robust bulk loader for SEC IAPD **Registered Adviser**
files (Excel & CSV). Skips ERA files by default, supports parallel loads, and
connects securely to AWS RDS using SSL.

Key fixes v1.5
--------------
* **Enforce SSL** on all database connections (`?sslmode=require`).
* Retains .env support, header coercion, stub skip, parallel ingestion.

Usage
-----
    python3 load_iapd_to_postgres.py [SRC_DIR] [--workers N] [--include-exempt]

Env vars or .env
----------------
    PGHOST       RDS endpoint
    PGPORT       5432
    PGDATABASE   iapd
    PGUSER       iapdadmin
    PGPASSWORD   AdvPwd#2025

Install deps
------------
    pip3 install pandas openpyxl sqlalchemy psycopg2-binary tqdm python-dotenv
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

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ModuleNotFoundError:
    pass

# Header variants map
HEADER_VARIANTS = {
    "crd": ["FirmCrdNb"],
    "filing_date": ["FilingDate"],
    "raum": ["RAUM_5B2", "RegulatoryAssetsUnderManagement", "Total_RAUM"],
    "total_accounts": ["Item5F2f_TotalAccts", "TotalNumberOfAccounts"],
    "disclosure_flag": ["Item11DisclosureFlag", "HasDisciplinaryHistory"],
}
CLIENT_BUCKET_PREFIX = "Item5D_1"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ia_filing (
  crd             INT,
  filing_date     DATE,
  raum            NUMERIC,
  total_clients   INT,
  total_accounts  INT,
  cco_id          TEXT,
  disclosure_flag CHAR(1),
  PRIMARY KEY (crd, filing_date)
);
"""

# Database connection with SSL enforcement
def get_engine() -> sa.Engine:
    for v in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        if not os.getenv(v):
            sys.exit(f"{v} not set (define in shell or .env)")
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT", "5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    # Build URL with SSL mode
    url = (
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        f"?sslmode=require"
    )
    return sa.create_engine(url, pool_pre_ping=True)

# Read Excel or CSV, skip macOS stubs
def read_any(path: Path) -> pd.DataFrame:
    if path.name.startswith("._"):
        raise RuntimeError("resource-fork stub – skip")
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    if ext == ".csv":
        return pd.read_csv(path, dtype=str, sep=",|\|", engine="python")
    raise ValueError(f"Unsupported extension: {path}")

# Pick first matching header
def pick_column(df: pd.DataFrame, variants: List[str]) -> str | None:
    for c in variants:
        if c in df.columns:
            return c
    return None

# Normalize DataFrame to canonical schema
def normalise(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.map(str)
    out = pd.DataFrame()
    for canon, variants in HEADER_VARIANTS.items():
        col = pick_column(df, variants)
        out[canon] = df[col] if col else None
    # Cast numerics
    for c in ("raum", "total_accounts"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    # Compute total_clients
    buckets = [c for c in df.columns if c.startswith(CLIENT_BUCKET_PREFIX)]
    out["total_clients"] = (
        df[buckets].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        if buckets else None
    )
    # Composite cco_id
    if {"ChiefComplianceOfficer_FirstName","ChiefComplianceOfficer_LastName","ChiefComplianceOfficer_CRD"}.issubset(df.columns):
        out["cco_id"] = (
            df["ChiefComplianceOfficer_FirstName"].fillna("").str.strip().str.upper()
            + "|" + df["ChiefComplianceOfficer_LastName"].fillna("").str.strip().str.upper()
            + "|" + df["ChiefComplianceOfficer_CRD"].fillna("")
        )
    else:
        out["cco_id"] = None
    return out

# Ingest one file via SQLAlchemy
def ingest_file(path: Path, dsn: str) -> str:
    try:
        engine = sa.create_engine(dsn, pool_pre_ping=True)
        raw = read_any(path)
        df = normalise(raw)
        df.to_sql("ia_filing", engine, if_exists="append", index=False,
                  method="multi", chunksize=20000)
        return f"✓ {path.name}: {len(df)} rows"
    except RuntimeError as skip:
        return f"· {path.name}: {skip}"
    except Exception as exc:
        return f"✗ {path.name}: {exc}"

# Main entry point
def main():
    p = argparse.ArgumentParser(description="Load SEC IAPD files into Postgres")
    p.add_argument("src_dir", help="Local dir of XLSX/CSV to ingest")
    p.add_argument("--workers", type=int, default=1, help="Parallel processes")
    p.add_argument("--include-exempt", action="store_true", help="Include ERA files")
    args = p.parse_args()
    src = Path(args.src_dir).expanduser().resolve()
    if not src.exists(): sys.exit(f"Source dir not found: {src}")
    # Collect files
    files = [p for ext in ("*.xlsx","*.xls","*.csv") for p in src.rglob(ext)]
    if not args.include_exempt:
        files = [f for f in files if "exempt" not in f.name.lower()]
    if not files: sys.exit("No data files found after filtering")
    # Prepare DSN
    engine = get_engine()
    # Create table if missing
    with engine.begin() as conn: conn.execute(sa.text(CREATE_SQL))
    dsn = engine.url + "?sslmode=require"
    print(f"Ingesting {len(files)} files → ia_filing using {args.workers} worker(s)\n")
    # Choose execution mode
    if args.workers == 1:
        for f in tqdm(files, unit="file"): print(ingest_file(f, dsn))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(ingest_file, f, dsn): f for f in files}
            for fut in tqdm(as_completed(futures), total=len(files), unit="file"):
                print(fut.result())
    print("\nDone ✔ – check ia_filing for rows.")

if __name__ == "__main__":
    main()
