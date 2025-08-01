#!/usr/bin/env python3
"""
Simple IAPD data loader - handles both CSV and Excel files
Only loads data from 2020 onwards for faster processing
"""
import pandas as pd
import sqlite3
from pathlib import Path
import re

def clean_column_name(col):
    """Standardize column names"""
    col = str(col).strip()
    # Remove extra spaces and standardize
    col = re.sub(r'\s+', ' ', col)
    col = col.replace('Organization CRD #', 'CRD')
    col = col.replace('Organization CRD#', 'CRD')
    col = col.replace('Primary Business Name', 'Firm_Name')
    return col

def read_file(file_path):
    """Read either CSV or Excel file"""
    try:
        if file_path.suffix.lower() == '.csv':
            # Try different encodings for CSV files
            for encoding in ['latin1', 'utf-8']:
                try:
                    return pd.read_csv(file_path, dtype=str, encoding=encoding, on_bad_lines='skip')
                except:
                    continue
            return None
        elif file_path.suffix.lower() == '.xlsx':
            return pd.read_excel(file_path, dtype=str)
        else:
            return None
    except Exception as e:
        print(f"Error reading {file_path.name}: {e}")
        return None

def extract_key_data(df, filename):
    """Extract key data from a dataframe"""
    if df is None or df.empty:
        return []
    
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Find key columns
    crd_col = None
    name_col = None
    aum_col = None
    client_col = None
    disclosure_cols = []
    
    for col in df.columns:
        col_lower = col.lower()
        if 'crd' in col_lower and not crd_col:
            crd_col = col
        elif 'name' in col_lower and not name_col:
            name_col = col
        elif 'aum' in col_lower or 'assets' in col_lower:
            aum_col = col
        elif 'client' in col_lower:
            client_col = col
        elif 'disclosure' in col_lower:
            disclosure_cols.append(col)
    
    # Extract data
    result = []
    for _, row in df.iterrows():
        try:
            crd = row.get(crd_col)
            if pd.isna(crd) or crd == '':
                continue
                
            record = {
                'crd': int(crd) if crd else None,
                'firm_name': str(row.get(name_col, ''))[:100],
                'aum': row.get(aum_col),
                'total_clients': row.get(client_col),
                'filename': filename,
                'disclosure_count': sum(1 for col in disclosure_cols if row.get(col) and str(row.get(col)).lower() in ['yes', 'true', '1'])
            }
            result.append(record)
        except:
            continue
    
    return result

def get_file_year(filename):
    """Extract year from filename like 'ia010120.zip' -> 2020"""
    # Pattern: iaMMDDYY.zip or iaMMDDYYYY.zip
    match = re.search(r'ia(\d{6,8})\.zip', filename)
    if match:
        date_str = match.group(1)
        if len(date_str) == 6:  # MMDDYY format
            year = int('20' + date_str[4:6])
        else:  # MMDDYYYY format
            year = int(date_str[4:8])
        return year
    return None

def simple_load():
    """Load data from 2020 onwards into SQLite"""
    data_dir = Path("data/unzipped/iapd")
    
    # Get all data files (CSV and Excel)
    csv_files = list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))
    excel_files = list(data_dir.glob("*.xlsx"))
    all_files = csv_files + excel_files
    all_files = [f for f in all_files if not f.name.startswith('~$')]
    
    # Filter for 2020 onwards
    recent_files = []
    for file_path in all_files:
        # Convert filename back to ZIP filename pattern
        zip_name = file_path.stem + '.zip'
        year = get_file_year(zip_name)
        if year and year >= 2020:
            recent_files.append(file_path)
    
    print(f"Found {len(all_files)} total data files")
    print(f"Processing {len(recent_files)} files from 2020 onwards...")
    
    # Show date range
    if recent_files:
        years = [get_file_year(f.stem + '.zip') for f in recent_files if get_file_year(f.stem + '.zip')]
        print(f"Date range: {min(years)} - {max(years)}")
    
    # Create SQLite database
    conn = sqlite3.connect('iapd_data.db')
    
    # Create table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS firms (
            id INTEGER PRIMARY KEY,
            crd INTEGER,
            firm_name TEXT,
            aum REAL,
            total_clients INTEGER,
            filename TEXT,
            disclosure_count INTEGER,
            file_year INTEGER
        )
    ''')
    
    total_records = 0
    
    for i, file_path in enumerate(recent_files):
        if i % 5 == 0:
            print(f"Processing file {i+1}/{len(recent_files)}: {file_path.name}")
        
        try:
            df = read_file(file_path)
            records = extract_key_data(df, file_path.name)
            
            # Add year to records
            zip_name = file_path.stem + '.zip'
            year = get_file_year(zip_name)
            
            # Insert into database
            for record in records:
                conn.execute('''
                    INSERT INTO firms (crd, firm_name, aum, total_clients, filename, disclosure_count, file_year)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (record['crd'], record['firm_name'], record['aum'], 
                     record['total_clients'], record['filename'], record['disclosure_count'], year))
            
            total_records += len(records)
            
        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\nDone! Loaded {total_records} records into iapd_data.db")
    
    # Show some stats
    conn = sqlite3.connect('iapd_data.db')
    stats = conn.execute('SELECT COUNT(*) as total, COUNT(DISTINCT crd) as unique_firms FROM firms').fetchone()
    print(f"Total records: {stats[0]}")
    print(f"Unique firms: {stats[1]}")
    
    # Show year distribution
    year_stats = conn.execute('SELECT file_year, COUNT(*) FROM firms GROUP BY file_year ORDER BY file_year').fetchall()
    print(f"\nRecords by year:")
    for year, count in year_stats:
        print(f"  {year}: {count:,} records")
    
    conn.close()

if __name__ == "__main__":
    simple_load() 