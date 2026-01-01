#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Test a few different files to see their filing dates
test_files = [
    "data/unzipped/iapd/ia010220.xlsx",  # 2020 file
    "data/unzipped/iapd/ia010322.csv",   # 2022 file
    "data/unzipped/iapd/ia010324.xlsx",  # 2024 file
]

for file_path in test_files:
    path = Path(file_path)
    print(f"\n=== {path.name} ===")
    
    try:
        if path.suffix.lower() == '.csv':
            df = pd.read_csv(path, sep=",", encoding="latin1", low_memory=False, on_bad_lines='skip')
        else:
            df = pd.read_excel(path, engine='openpyxl')
            
        # Look for filing date columns
        filing_date_cols = [col for col in df.columns if isinstance(col, str) and ('filing' in col.lower() or 'latest' in col.lower())]
        print(f"Filing date columns: {filing_date_cols}")
        
        # Check the actual values
        for col in filing_date_cols:
            unique_dates = df[col].dropna().unique()
            print(f"  {col}: {len(unique_dates)} unique values")
            if len(unique_dates) <= 5:
                print(f"    Values: {unique_dates}")
            else:
                print(f"    First 5: {unique_dates[:5]}")
                
    except Exception as e:
        print(f"Error: {e}") 