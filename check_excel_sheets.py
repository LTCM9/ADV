#!/usr/bin/env python3
"""
Check which Excel files have multiple sheets
"""
import os
from pathlib import Path
import pandas as pd

def check_excel_sheets():
    data_dir = Path("data/unzipped/iapd")
    
    if not data_dir.exists():
        print("Data directory not found!")
        return
    
    excel_files = list(data_dir.glob("*.xlsx")) + list(data_dir.glob("*.xls"))
    
    print(f"Found {len(excel_files)} Excel files")
    print("\nFiles with multiple sheets:")
    print("-" * 50)
    
    multi_sheet_files = []
    
    for file_path in excel_files:
        try:
            # Read all sheet names without loading the data
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            if len(sheet_names) > 1:
                print(f"{file_path.name}: {len(sheet_names)} sheets")
                print(f"  Sheets: {', '.join(sheet_names)}")
                print()
                multi_sheet_files.append((file_path.name, sheet_names))
            else:
                print(f"{file_path.name}: 1 sheet ({sheet_names[0]})")
                
        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")
    
    print(f"\nSummary: {len(multi_sheet_files)} files have multiple sheets")
    
    return multi_sheet_files

if __name__ == "__main__":
    check_excel_sheets() 