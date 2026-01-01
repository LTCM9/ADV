#!/usr/bin/env python3
"""
Examine the structure of extracted SEC IAPD data files
"""
import os
from pathlib import Path
import pandas as pd

def examine_data_structure():
    data_dir = Path("data/unzipped/iapd")
    
    if not data_dir.exists():
        print("Data directory not found!")
        return
    
    # Get a few sample files to examine
    excel_files = list(data_dir.glob("*.xlsx"))[:2]  # First 2 Excel files
    csv_files = list(data_dir.glob("*.csv"))[:2]     # First 2 CSV files
    
    print("Examining Excel files:")
    print("-" * 50)
    
    for file_path in excel_files:
        try:
            print(f"\nFile: {file_path.name}")
            df = pd.read_excel(file_path, nrows=5)  # Read first 5 rows
            print(f"Shape: {df.shape}")
            
            # Look for CRD and SEC number columns
            crd_cols = [col for col in df.columns if 'crd' in str(col).lower()]
            sec_cols = [col for col in df.columns if 'sec' in str(col).lower()]
            
            if crd_cols:
                print(f"CRD columns: {crd_cols}")
                for col in crd_cols:
                    print(f"  {col}: {df[col].head().tolist()}")
            
            if sec_cols:
                print(f"SEC columns: {sec_cols}")
                for col in sec_cols:
                    print(f"  {col}: {df[col].head().tolist()}")
                    
        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")
    
    print("\n" + "="*50)
    print("Examining CSV files:")
    print("-" * 50)
    
    for file_path in csv_files:
        try:
            print(f"\nFile: {file_path.name}")
            df = pd.read_csv(file_path, nrows=5)  # Read first 5 rows
            print(f"Shape: {df.shape}")
            
            # Look for CRD and SEC number columns
            crd_cols = [col for col in df.columns if 'crd' in str(col).lower()]
            sec_cols = [col for col in df.columns if 'sec' in str(col).lower()]
            
            if crd_cols:
                print(f"CRD columns: {crd_cols}")
                for col in crd_cols:
                    print(f"  {col}: {df[col].head().tolist()}")
            
            if sec_cols:
                print(f"SEC columns: {sec_cols}")
                for col in sec_cols:
                    print(f"  {col}: {df[col].head().tolist()}")
                    
        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")

if __name__ == "__main__":
    examine_data_structure() 