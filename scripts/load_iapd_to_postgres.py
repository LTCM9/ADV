#!/usr/bin/env python3
"""
load_iapd_to_postgres.py – Bulk loader for SEC IAPD files from local disk or S3 into Postgres.

Features:
* Ingests Excel (.xlsx/.xls) and CSV files including from S3 URIs (`s3://bucket/prefix`).
* Skips macOS resource-fork stubs and ERA files by default.
* Supports parallel loading with configurable workers.
* Enforces SSL on RDS connections via `sslmode=require`.
* Auto-loads `.env` credentials for both AWS and Postgres.

Usage
-----
  python3 load_iapd_to_postgres.py <SRC> [--workers N] [--include-exempt]

  <SRC> can be:
    • a local directory path holding .xlsx/.xls/.csv files
    • an S3 URI like s3://adv-iapd-raw/extracted

Options
-------
  --workers        Number of parallel processes (default: 1)
  --include-exempt Include files with "exempt" in their names

Env vars / .env
---------------
# PostgreSQL
PGHOST       RDS endpoint
PGPORT       5432 (optional)
PGDATABASE   iapd
PGUSER       iapdadmin
PGPASSWORD   AdvPwd#2025

# AWS (for S3 access)
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION

Install dependencies
--------------------
  pip3 install pandas openpyxl sqlalchemy psycopg2-binary tqdm python-dotenv boto3
"""
from __future__ import annotations
import argparse
import os
import sys
import io
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Union, Tuple

import boto3
import pandas as pd
import sqlalchemy as sa
from tqdm import tqdm

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
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

# Initialize AWS S3 client
s3 = boto3.client("s3")

# Database engine factory enforcing SSL
def get_engine() -> sa.Engine:
    for v in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        if not os.getenv(v):
            sys.exit(f"Environment variable {v} not set (shell or .env)")
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT", "5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    url = (
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"
    )
    return sa.create_engine(url, pool_pre_ping=True)

# Pick first matching header
def pick_column(df: pd.DataFrame, variants: List[str]) -> Union[str, None]:
    for c in variants:
        if c in df.columns:
            return c
    return None

# Normalize DataFrame
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

# Read a local file
def read_local(path: Path) -> pd.DataFrame:
    if path.name.startswith("._"):
        raise RuntimeError("resource-fork stub – skip")
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    if ext == ".csv":
        return pd.read_csv(path, dtype=str, sep=",|\|", engine="python")
    raise ValueError(f"Unsupported extension: {path}")

# Read from S3 into DataFrame
def read_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    if key.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(buf, dtype=str, engine="openpyxl")
    return pd.read_csv(buf, dtype=str, sep=",|\|", engine="python")

# Ingest one local file
def ingest_local(path: Path, dsn: str) -> str:
    try:
        df = normalise(read_local(path))
        engine = sa.create_engine(dsn, pool_pre_ping=True)
        df.to_sql("ia_filing", engine, if_exists="append", index=False,
                  method="multi", chunksize=20000)
        return f"✓ {path.name}: {len(df)} rows"
    except Exception as e:
        return f"✗ {path.name}: {e}"

# Ingest one S3 object
def ingest_s3(bucket: str, key: str, dsn: str) -> str:
    name = Path(key).name
    try:
        df = normalise(read_s3(bucket, key))
        engine = sa.create_engine(dsn, pool_pre_ping=True)
        df.to_sql("ia_filing", engine, if_exists="append", index=False,
                  method="multi", chunksize=20000)
        return f"✓ {name}: {len(df)} rows"
    except Exception as e:
        return f"✗ {name}: {e}"

# List S3 keys under prefix
def list_s3_keys(bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

# Main
def main():
    p = argparse.ArgumentParser(description="Load SEC IAPD files into Postgres")
    p.add_argument("src", help="Local dir or s3://bucket/prefix")
    p.add_argument("--workers", type=int, default=1, help="Parallel processes")
    p.add_argument("--include-exempt", action="store_true", help="Include ERA files")
    args = p.parse_args()

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(sa.text(CREATE_SQL))
    dsn = engine.url
    tasks: List[Tuple[str, Union[Path, Tuple[str,str]]]] = []

    if args.src.lower().startswith("s3://"):
        # parse bucket/prefix
        _, _, rest = args.src.partition("s3://")
        bucket, _, prefix = rest.partition("/")
        keys = list_s3_keys(bucket, prefix)
        for key in keys:
            if not args.include_exempt and "exempt" in key.lower():
                continue
            if key.lower().endswith((".xlsx",".xls",".csv")):
                tasks.append((key, (bucket, key)))
    else:
        src_dir = Path(args.src).expanduser().resolve()
        if not src_dir.is_dir():
            sys.exit(f"Source dir not found: {src_dir}")
        for ext in ("*.xlsx","*.xls","*.csv"):
            for path in src_dir.rglob(ext):
                if not args.include_exempt and "exempt" in path.name.lower():
                    continue
                tasks.append((path.name, path))

    if not tasks:
        sys.exit("No data files found for ingestion.")

    print(f"Ingesting {len(tasks)} files → ia_filing using {args.workers} worker(s)\n")

    if args.workers == 1:
        for name, src in tqdm(tasks, unit="file"):
            if isinstance(src, Path):
                print(ingest_local(src, str(dsn)))
            else:
                bucket, key = src
                print(ingest_s3(bucket, key, str(dsn)))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {}
            for name, src in tasks:
                if isinstance(src, Path):
                    futures[pool.submit(ingest_local, src, str(dsn))] = name
                else:
                    bucket, key = src
                    futures[pool.submit(ingest_s3, bucket, key, str(dsn))] = name
            for fut in tqdm(as_completed(futures), total=len(tasks), unit="file"):
                print(fut.result())

    print("\nDone ✔ – check ia_filing for rows.")

if __name__ == "__main__":
    main()
