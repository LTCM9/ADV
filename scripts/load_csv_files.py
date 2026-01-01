#!/usr/bin/env python3
"""
load_csv_files.py – Dedicated loader for SEC IAPD CSV files only
"""
import os
import sys
import argparse
from pathlib import Path
import pandas as pd
import sqlalchemy as sa
from tqdm import tqdm
import re

# Field mapping for CSV files (same as Excel but with CSV-specific handling)
FIELD_MAPPING = {
    'sec_number': 'SEC#',
    'crd_number': 'Organization CRD#',
    'firm_name': 'Primary Business Name',
    'legal_name': 'Legal Name',
    'sec_region': 'SEC Region',
    'sec_status': 'SEC Current Status',
    'sec_status_date': 'SEC Status Effective Date',
    'raum': '5F(2)(c)',  # Regulatory Assets Under Management
    # Client count will be calculated by summing 5D fields
    'account_count': '5F(2)(f)',  # Number of accounts
    'cco_name': 'Chief Compliance Officer Name',
    'cco_phone': 'Chief Compliance Officer Telephone',
    'cco_email': 'Chief Compliance Officer E-mail',
    'firm_type': 'Firm Type',
    'umbrella_registration': 'Umbrella Registration',
    'website': 'Website Address',
    'main_office_city': 'Main Office City',
    'main_office_state': 'Main Office State',
    'main_office_country': 'Main Office Country',
}

def get_dsn_and_engine() -> tuple[str, sa.Engine]:
    """Get database connection string and engine"""
    for var in ("PGHOST","PGDATABASE","PGUSER","PGPASSWORD"):
        if not os.getenv(var):
            sys.exit(f"Missing environment variable: {var}")
    
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT","5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    
    dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    # Use SSL only for remote connections, disable for local and Docker
    ssl_mode = "disable" if host in ["localhost", "127.0.0.1", "postgres"] else "require"
    engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": ssl_mode})
    return dsn, engine

def extract_filing_date(filename: str) -> str:
    """Extract filing date from filename like 'ia07012025.csv' -> '2025-07-01'"""
    match = re.search(r'ia(\d{2})(\d{2})(\d{4})', filename)
    if match:
        month, day, year = match.groups()
        return f"{year}-{month}-{day}"
    return None

def count_disciplinary_disclosures(df: pd.DataFrame) -> int:
    """Count Section 11 disciplinary disclosure fields"""
    section_11_cols = [col for col in df.columns if col.startswith('11') and 'Count' in col]
    if section_11_cols:
        return df[section_11_cols].sum(axis=1).fillna(0).astype(int)
    return 0

def normalize_dataframe(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Normalize the DataFrame to match our database schema"""
    df.columns = df.columns.map(str)
    
    # Create output DataFrame
    out = pd.DataFrame()
    
    # Map fields from the source data
    for db_field, source_field in FIELD_MAPPING.items():
        if source_field in df.columns:
            out[db_field] = df[source_field]
        else:
            out[db_field] = None
    
    # Extract filing date from filename
    filing_date = extract_filing_date(filename)
    if filing_date:
        out['filing_date'] = filing_date
    
    # Convert numeric fields
    if 'raum' in out.columns:
        out['raum'] = pd.to_numeric(out['raum'], errors='coerce')
    
    # Calculate client count by summing 5D fields (5D(a)(1) through 5D(n)(1))
    client_columns = []
    for letter in 'abcdefghijklmn':
        col_name = f'5D({letter})(1)'
        if col_name in df.columns:
            client_columns.append(col_name)
    
    if client_columns:
        out['client_count'] = df[client_columns].apply(pd.to_numeric, errors='coerce').sum(axis=1)
    else:
        out['client_count'] = 0
    
    if 'account_count' in out.columns:
        out['account_count'] = pd.to_numeric(out['account_count'], errors='coerce')
    
    # Convert boolean fields
    if 'umbrella_registration' in out.columns:
        out['umbrella_registration'] = out['umbrella_registration'].map({'Y': True, 'N': False, 'Yes': True, 'No': False})
    
    # Count disciplinary disclosures (Section 11)
    out['disciplinary_disclosures'] = count_disciplinary_disclosures(df)
    
    # Clean up SEC number format
    if 'sec_number' in out.columns:
        out['sec_number'] = out['sec_number'].astype(str).str.strip()
    
    # Clean up CRD number format (keep as string to preserve leading zeros)
    if 'crd_number' in out.columns:
        out['crd_number'] = out['crd_number'].astype(str).str.strip()
    
    return out

def read_csv_file(path: Path) -> pd.DataFrame:
    """Read a CSV file with proper encoding handling"""
    try:
        # Use latin-1 encoding and comma separator (tested and working)
        return pd.read_csv(path, sep=",", encoding="latin1", low_memory=False, on_bad_lines='skip')
    except Exception as e:
        print(f"Error reading {path}: {e}")
        raise

def ingest_csv_file(name: str, df: pd.DataFrame, dsn: str) -> str:
    """Ingest CSV DataFrame into Postgres"""
    try:
        # Use SSL only for remote connections, disable for local and Docker
        host = dsn.split("@")[1].split(":")[0] if "@" in dsn else "localhost"
        ssl_mode = "disable" if host in ["localhost", "127.0.0.1", "postgres"] else "require"
        engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": ssl_mode})
        
        # Normalize the data
        clean = normalize_dataframe(df, name)
        
        # Filter out rows without SEC numbers
        clean = clean.dropna(subset=['sec_number'])
        
        if len(clean) == 0:
            return f"· {name}: No valid SEC numbers found"
        
        # Load into database
        clean.to_sql("ia_filing", engine, if_exists="append", index=False, method='multi')
        return f"✓ {name}: {len(clean)} rows loaded"
    except Exception as e:
        return f"✗ {name}: {e}"

def main():
    p = argparse.ArgumentParser(description="Load SEC IAPD CSV files into PostgreSQL")
    p.add_argument("src", help="Directory containing CSV files")
    args = p.parse_args()

    dsn, engine = get_dsn_and_engine()
    
    src_dir = Path(args.src).expanduser().resolve()
    if not src_dir.is_dir():
        sys.exit(f"Source directory not found: {src_dir}")
    
    # Find all CSV files
    csv_files = list(src_dir.glob("*.csv"))
    
    if not csv_files:
        sys.exit("No CSV files found.")
    
    print(f"Found {len(csv_files)} CSV files to process...\n")
    
    # Process all CSV files
    for i, csv_file in enumerate(csv_files):
        print(f"Processing {i+1}/{len(csv_files)}: {csv_file.name}")
        try:
            df = read_csv_file(csv_file)
            result = ingest_csv_file(csv_file.name, df, dsn)
            print(result)
        except Exception as e:
            print(f"✗ {csv_file.name}: {e}")
    
    print("\nDone ✔")

if __name__ == "__main__":
    main() 