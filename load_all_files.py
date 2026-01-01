#!/usr/bin/env python3
import os
import sys
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
from tqdm import tqdm

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

def load_file(file_path):
    """Load a single file and return success status and record count"""
    try:
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path, sep=",", encoding="latin1", low_memory=False, on_bad_lines='skip')
        else:
            df = pd.read_excel(file_path, engine='openpyxl')
        
        # Basic mapping (simplified version)
        if 'SEC#' in df.columns:
            df['sec_number'] = df['SEC#']
        if 'Primary Business Name' in df.columns:
            df['firm_name'] = df['Primary Business Name']
        if 'Latest ADV Filing Date' in df.columns:
            df['filing_date'] = pd.to_datetime(df['Latest ADV Filing Date'], errors='coerce')
        
        # Filter out rows without required fields
        df = df.dropna(subset=['sec_number', 'filing_date'])
        
        if len(df) == 0:
            return False, 0, "No valid records"
        
        # Load to database
        df[['sec_number', 'firm_name', 'filing_date']].to_sql(
            'ia_filing', engine, if_exists='append', index=False, method='multi'
        )
        
        return True, len(df), "Success"
        
    except Exception as e:
        return False, 0, str(e)

# Get all files
data_dir = Path("data/unzipped/iapd")
files = list(data_dir.glob("*.xlsx")) + list(data_dir.glob("*.csv"))
print(f"Found {len(files)} files to process")

# Track results
results = []
success_count = 0
total_records = 0

for file_path in tqdm(files, desc="Processing files"):
    success, count, message = load_file(file_path)
    results.append({
        'file': file_path.name,
        'success': success,
        'count': count,
        'message': message
    })
    
    if success:
        success_count += 1
        total_records += count

# Print summary
print(f"\n=== SUMMARY ===")
print(f"Files processed: {len(files)}")
print(f"Files succeeded: {success_count}")
print(f"Files failed: {len(files) - success_count}")
print(f"Total records loaded: {total_records}")

# Show failed files
failed_files = [r for r in results if not r['success']]
if failed_files:
    print(f"\n=== FAILED FILES ===")
    for r in failed_files[:10]:  # Show first 10
        print(f"  {r['file']}: {r['message']}")
    if len(failed_files) > 10:
        print(f"  ... and {len(failed_files) - 10} more") 