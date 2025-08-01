#!/usr/bin/env python3
"""
Simple analysis of IAPD Excel files - no overengineering
"""
import pandas as pd
import os
from pathlib import Path
from collections import defaultdict

def simple_analysis():
    """Just read the files and see what we have"""
    data_dir = Path("data/unzipped/iapd")
    excel_files = list(data_dir.glob("*.xlsx"))
    excel_files = [f for f in excel_files if not f.name.startswith('~$')]  # Skip temp files
    
    print(f"Found {len(excel_files)} Excel files")
    
    # Sample a few files to understand structure
    sample_files = excel_files[:3]
    
    for file_path in sample_files:
        print(f"\n=== {file_path.name} ===")
        try:
            df = pd.read_excel(file_path)
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)[:10]}...")  # First 10 columns
            
            # Look for key columns
            key_columns = ['CRD', 'AUM', 'Client', 'Account', 'CCO', 'Disclosure']
            found_columns = []
            for col in df.columns:
                for key in key_columns:
                    if key.lower() in str(col).lower():
                        found_columns.append(col)
            
            if found_columns:
                print(f"Key columns found: {found_columns}")
            
        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")

if __name__ == "__main__":
    simple_analysis() 