#!/usr/bin/env python3
import pandas as pd
import os

# Check the source data to understand the 5F(2)(f) column
extracted_dir = "data/unzipped/iapd"

if os.path.exists(extracted_dir):
    # Look at a few Excel files to see what the 5F(2)(f) column contains
    excel_files = [f for f in os.listdir(extracted_dir) if f.endswith('.xlsx')][:2]
    
    for excel_file in excel_files:
        file_path = os.path.join(extracted_dir, excel_file)
        print(f"\n📊 File: {excel_file}")
        try:
            df = pd.read_excel(file_path)
            
            # Check if 5F(2)(f) column exists
            if '5F(2)(f)' in df.columns:
                print(f"\n  5F(2)(f) column analysis:")
                print(f"    Total rows: {len(df)}")
                print(f"    Non-null values: {df['5F(2)(f)'].notna().sum()}")
                print(f"    Unique values: {df['5F(2)(f)'].unique()}")
                print(f"    Sample values: {df['5F(2)(f)'].dropna().head(10).tolist()}")
                
                # Show some sample rows with account count data
                sample_cols = ['SEC#', 'Primary Business Name', '5F(2)(f)']
                print(f"\n  Sample rows with account count data:")
                print(df[sample_cols].head(5).to_string())
            else:
                print(f"  ❌ 5F(2)(f) column not found!")
                
                # Look for any 5F-related columns
                f5_columns = [col for col in df.columns if '5F' in str(col)]
                print(f"  5F-related columns found: {f5_columns}")
            
        except Exception as e:
            print(f"  Error reading {excel_file}: {e}")
else:
    print(f"❌ Extracted directory {extracted_dir} not found") 