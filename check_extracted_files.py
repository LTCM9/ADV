#!/usr/bin/env python3
"""
Check how many files were extracted and which ZIP files contained multiple files
"""
import os
from pathlib import Path
import zipfile

def check_extracted_files():
    zip_dir = Path("data/raw/iapd")
    extracted_dir = Path("data/unzipped/iapd")
    
    if not zip_dir.exists():
        print("ZIP directory not found!")
        return
    
    if not extracted_dir.exists():
        print("Extracted directory not found!")
        return
    
    # Count extracted files
    extracted_files = list(extracted_dir.glob("*"))
    excel_files = list(extracted_dir.glob("*.xlsx")) + list(extracted_dir.glob("*.xls"))
    csv_files = list(extracted_dir.glob("*.csv"))
    other_files = [f for f in extracted_files if f.suffix.lower() not in ['.xlsx', '.xls', '.csv']]
    
    print(f"Total extracted files: {len(extracted_files)}")
    print(f"Excel files: {len(excel_files)}")
    print(f"CSV files: {len(csv_files)}")
    print(f"Other files: {len(other_files)}")
    print()
    
    # Check which ZIP files contained multiple files
    print("ZIP files with multiple contents:")
    print("-" * 50)
    
    multi_file_zips = []
    
    for zip_file in zip_dir.glob("*.zip"):
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                file_list = zf.namelist()
                
                # Filter out macOS resource fork files and directories
                actual_files = [f for f in file_list if not f.startswith('__MACOSX/') and not f.endswith('/')]
                
                if len(actual_files) > 1:
                    print(f"{zip_file.name}: {len(actual_files)} files")
                    for file_name in actual_files:
                        print(f"  - {file_name}")
                    print()
                    multi_file_zips.append((zip_file.name, actual_files))
                else:
                    print(f"{zip_file.name}: 1 file ({actual_files[0] if actual_files else 'empty'})")
                    
        except Exception as e:
            print(f"Error reading {zip_file.name}: {e}")
    
    print(f"\nSummary: {len(multi_file_zips)} ZIP files contained multiple files")
    
    return multi_file_zips

if __name__ == "__main__":
    check_extracted_files() 