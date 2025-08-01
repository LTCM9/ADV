#!/usr/bin/env python3
"""
load_iapd_to_postgres.py – Bulk loader for SEC IAPD files from local disk or S3 into Postgres,
with robust error handling for network and encoding issues.

Features:
* Ingests Excel (.xlsx/.xls) and CSV files from local dirs or S3 URIs.
* Skips macOS resource-fork stubs and ERA files by default.
* Retries S3 reads on IncompleteRead, with fallbacks for CSV encodings.
* Supports threaded loading with configurable workers.
* Enforces SSL on RDS connections via `sslmode=require`.
* Auto-loads credentials from .env for both AWS and Postgres.

Usage
-----
  python3 load_iapd_to_postgres.py <SRC> [--workers N] [--include-exempt]

  <SRC> can be:
    • a local directory path holding .xlsx/.xls/.csv files
    • an S3 URI like s3://adv-iapd-raw/extracted

Options
-------
  --workers        Number of parallel threads (default: 1)
  --include-exempt Include files with "exempt" in their names

Env vars / .env
---------------
PGHOST               RDS endpoint
PGPORT               5432 (optional)
PGDATABASE           iapd
PGUSER               iapdadmin
PGPASSWORD           AdvPwd#2025
AWS_ACCESS_KEY_ID    AWS key
AWS_SECRET_ACCESS_KEY AWS secret
AWS_REGION           AWS region

Install dependencies
--------------------
  pip3 install pandas openpyxl sqlalchemy psycopg2-binary tqdm python-dotenv boto3 botocore
"""
from __future__ import annotations
import argparse, os, sys, io
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Union, Tuple

import boto3, botocore
import pandas as pd
import sqlalchemy as sa
from tqdm import tqdm

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

# Header mapping
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

# Build DSN string and engine factory
def get_dsn_and_engine() -> Tuple[str, sa.Engine]:
    for var in ("PGHOST","PGDATABASE","PGUSER","PGPASSWORD"):
        if not os.getenv(var):
            sys.exit(f"Missing environment variable: {var}")
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT","5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    # Use SSL only for remote connections (RDS), disable for local
    ssl_mode = "require" if host != "localhost" else "disable"
    engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": ssl_mode})
    return dsn, engine

# Helper to pick a header
def pick_column(df: pd.DataFrame, variants: List[str]) -> Union[str, None]:
    for v in variants:
        if v in df.columns:
            return v
    return None

# Normalize DataFrame
def normalise(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.map(str)
    out = pd.DataFrame()
    for canon, variants in HEADER_VARIANTS.items():
        col = pick_column(df, variants)
        out[canon] = df[col] if col else None
    for c in ("raum","total_accounts"): out[c] = pd.to_numeric(out[c], errors="coerce")
    buckets = [c for c in df.columns if c.startswith(CLIENT_BUCKET_PREFIX)]
    if buckets:
        bucket_data = df[buckets]
        for col in buckets:
            bucket_data[col] = pd.to_numeric(bucket_data[col], errors="coerce")
        out["total_clients"] = bucket_data.sum(axis=1)
    else:
        out["total_clients"] = None
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
    if path.name.startswith("._"): raise RuntimeError("stub skip")
    ext = path.suffix.lower()
    if ext in (".xlsx",".xls"): 
        return pd.read_excel(path, engine="openpyxl")
    if ext == ".csv": return pd.read_csv(path, sep=",|\\|", engine="python", encoding="utf-8", errors="replace")
    raise ValueError(f"Unsupported file: {path}")

# Read from S3 with retry and encoding fallback
def read_s3(bucket: str, key: str) -> pd.DataFrame:
    if Path(key).name.startswith("._"): raise RuntimeError("stub skip")
    for attempt in range(2):
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            data = resp["Body"].read()
            break
        except botocore.exceptions.IncompleteRead as e:
            if attempt==1: raise
            data = e.partial or b""
    buf = io.BytesIO(data)
    if key.lower().endswith((".xlsx",".xls")):
        return pd.read_excel(buf, dtype=str, engine="openpyxl")
    try:
        return pd.read_csv(buf, dtype=str, sep=",|\\|", engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        buf.seek(0)
        return pd.read_csv(buf, dtype=str, sep=",|\\|", engine="python", encoding="latin1")

# Ingest DataFrame into Postgres
def ingest_df(name: str, df: pd.DataFrame, dsn: str) -> str:
    try:
        # Use SSL only for remote connections (RDS), disable for local
        host = dsn.split("@")[1].split(":")[0] if "@" in dsn else "localhost"
        ssl_mode = "require" if host != "localhost" else "disable"
        engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": ssl_mode})
        clean = normalise(df)
        with engine.connect() as conn:
            clean.to_sql("ia_filing", conn, if_exists="append", index=False, schema=None)
        return f"✓ {name}: {len(clean)} rows"
    except Exception as e:
        return f"✗ {name}: {e}"

# Process one task (local or s3)
def process_task(task: Tuple[str, Union[Path, Tuple[str,str]]], dsn: str) -> str:
    name, src = task
    # ▷ Skip any FOIA CSVs (they’re badly formatted and we don’t ingest them)
    if name.lower().endswith('.csv') and 'foia' in name.lower():
        return f"· {name}: skipping FOIA file"
    try:
        df = read_local(src) if isinstance(src, Path) else read_s3(src[0], src[1])
        return ingest_df(name, df, dsn)
    except RuntimeError as skip:
        return f"· {name}: {skip}"
    except Exception as e:
        return f"✗ {name}: {e}"

# Main entry
def main():
    p = argparse.ArgumentParser()
    p.add_argument("src", help="Local dir or s3://bucket/prefix")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--include-exempt", action="store_true")
    args = p.parse_args()

    dsn, engine = get_dsn_and_engine()
    with engine.begin() as conn:
        conn.execute(sa.text(CREATE_SQL))

    tasks: List[Tuple[str, Union[Path, Tuple[str,str]]]] = []
    if args.src.lower().startswith("s3://"):
        _,_,rest = args.src.partition("s3://")
        bucket, prefix = rest.split("/",1)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents",[]):
                key = obj["Key"]
                if not args.include_exempt and "exempt" in key.lower(): continue
                if key.lower().endswith((".xlsx",".xls",".csv")):
                    tasks.append((Path(key).name, (bucket, key)))
    else:
        src_dir = Path(args.src).expanduser().resolve()
        if not src_dir.is_dir(): sys.exit(f"Source dir not found: {src_dir}")
        for ext in ("*.xlsx","*.xls","*.csv"):
            for f in src_dir.rglob(ext):
                if not args.include_exempt and "exempt" in f.name.lower(): continue
                tasks.append((f.name, f))

    if not tasks:
        sys.exit("No files found.")
    print(f"Ingesting {len(tasks)} files with {args.workers} worker(s)...\n")

    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process_task, t, dsn): t for t in tasks}
            for fut in tqdm(as_completed(futures), total=len(tasks), unit="file"):
                print(fut.result())
    else:
        # Process all files
        for i, t in enumerate(tasks):
            print(f"Processing {i+1}/{len(tasks)}: {t[0]}")
            result = process_task(t, dsn)
            print(result)

    print("\nDone ✔")

if __name__ == "__main__":
    main()
