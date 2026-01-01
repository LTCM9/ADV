#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# Test a few different files to see what's in them
test_files = [
    "data/unzipped/iapd/ia010220.xlsx",  # 2020 file
    "data/unzipped/iapd/ia010322.csv",   # 2022 file
    "data/unzipped/iapd/ia010324.xlsx",  # 2024 file
]

for file_path in test_files:
    path = Path(file_path)
    print(f"\n=== Testing {path.name} ===")
    
    if not path.exists():
        print(f"❌ File does not exist: {path}")
        continue
        
    try:
        if path.suffix.lower() == '.csv':
            df = pd.read_csv(path, sep=",", encoding="latin1", low_memory=False, on_bad_lines='skip')
        else:
            df = pd.read_excel(path, engine='openpyxl')
            
        print(f"✅ Successfully read {path.name}")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)[:5]}...")
        
        # Check if there's a date column
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        print(f"   Date columns: {date_columns}")
        
        # Show first few rows
        print(f"   First few rows:")
        print(df.head(2).to_string())
        
    except Exception as e:
        print(f"❌ Error reading {path.name}: {e}") 