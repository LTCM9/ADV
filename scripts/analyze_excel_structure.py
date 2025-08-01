#!/usr/bin/env python3
"""
Analyze the structure of all Excel files to understand column variations
"""
import os
import sys
from pathlib import Path
import pandas as pd
from collections import defaultdict

def analyze_excel_structure():
    """Analyze all Excel files to understand column structure"""
    data_dir = Path("data/unzipped/iapd")
    
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        return
    
    # Get all Excel files
    excel_files = list(data_dir.glob("*.xlsx"))
    excel_files = [f for f in excel_files if not f.name.startswith('._')]
    
    print(f"Found {len(excel_files)} Excel files to analyze")
    
    # Track column names across files
    column_counts = defaultdict(int)
    file_columns = {}
    
    for excel_file in excel_files:
        try:
            # Try different sheet names
            sheet_names = [0, 'Sheet1', 'Data', 'Report']
            
            df = None
            for sheet in sheet_names:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str, engine='openpyxl')
                    if not df.empty:
                        break
                except Exception as e:
                    continue
            
            if df is not None and not df.empty:
                # Count column occurrences
                for col in df.columns:
                    column_counts[col] += 1
                
                file_columns[excel_file.name] = list(df.columns)
                print(f"✓ Analyzed {excel_file.name}: {len(df.columns)} columns")
            else:
                print(f"✗ Failed to read {excel_file.name}")
                
        except Exception as e:
            print(f"✗ Error analyzing {excel_file.name}: {str(e)[:100]}...")
    
    # Print column analysis
    print(f"\n=== COLUMN ANALYSIS ===")
    print(f"Total files analyzed: {len(file_columns)}")
    print(f"Unique columns found: {len(column_counts)}")
    
    print(f"\n=== MOST COMMON COLUMNS ===")
    sorted_columns = sorted(column_counts.items(), key=lambda x: x[1], reverse=True)
    for col, count in sorted_columns[:20]:
        print(f"{col}: {count} files")
    
    # Look for CCO-related columns
    print(f"\n=== CCO-RELATED COLUMNS ===")
    cco_columns = [col for col in column_counts.keys() if 'cco' in col.lower() or 'compliance' in col.lower()]
    for col in cco_columns:
        print(f"{col}: {column_counts[col]} files")
    
    # Look for disciplinary-related columns
    print(f"\n=== DISCIPLINARY-RELATED COLUMNS ===")
    disc_columns = [col for col in column_counts.keys() if 'disciplinary' in col.lower() or 'disclosure' in col.lower() or 'violation' in col.lower()]
    for col in disc_columns:
        print(f"{col}: {column_counts[col]} files")
    
    # Look for management-related columns
    print(f"\n=== MANAGEMENT-RELATED COLUMNS ===")
    mgmt_columns = [col for col in column_counts.keys() if 'owner' in col.lower() or 'director' in col.lower() or 'management' in col.lower()]
    for mgmt_columns:
        print(f"{col}: {column_counts[col]} files")

if __name__ == "__main__":
    analyze_excel_structure() 