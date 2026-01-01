#!/usr/bin/env python3
import sqlalchemy as sa
import pandas as pd
import os

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check what columns we actually have in ia_filing
        result = conn.execute(sa.text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'ia_filing'
            ORDER BY ordinal_position
        """))
        
        print("🔍 ia_filing table columns:")
        print("=" * 50)
        for row in result:
            col_name, data_type, nullable = row
            print(f"  {col_name}: {data_type} (nullable: {nullable})")
        print()
        
        # Check a few sample records to see what we have
        result = conn.execute(sa.text("""
            SELECT 
                sec_number,
                firm_name,
                raum,
                client_count,
                account_count,
                disciplinary_disclosures,
                filing_date
            FROM ia_filing 
            WHERE raum > 0
            ORDER BY raum DESC
            LIMIT 5
        """))
        
        print("🔍 Sample records from ia_filing:")
        print("=" * 50)
        for row in result:
            sec_num, firm_name, raum, client_count, account_count, disc, filing_date = row
            print(f"SEC#: {sec_num}")
            print(f"  Firm: {firm_name}")
            print(f"  RAUM: ${raum:,.0f}")
            print(f"  Client Count: {client_count}")
            print(f"  Account Count: {account_count}")
            print(f"  Disciplinary: {disc}")
            print(f"  Date: {filing_date}")
            print()

except Exception as e:
    print(f"❌ Error: {e}")

# Now let's check the source data to see what columns are available
print("🔍 Checking source data files...")
extracted_dir = "data/unzipped/iapd"

if os.path.exists(extracted_dir):
    # Look at a few Excel files to see what columns they have
    excel_files = [f for f in os.listdir(extracted_dir) if f.endswith('.xlsx')][:3]
    
    for excel_file in excel_files:
        file_path = os.path.join(extracted_dir, excel_file)
        print(f"\n📊 File: {excel_file}")
        try:
            df = pd.read_excel(file_path)
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            
            # Look for any columns that might contain account information
            account_related_cols = [col for col in df.columns if 'account' in col.lower() or 'acct' in col.lower()]
            if account_related_cols:
                print(f"  Account-related columns: {account_related_cols}")
            
            # Look for any columns that might contain client information
            client_related_cols = [col for col in df.columns if 'client' in col.lower()]
            if client_related_cols:
                print(f"  Client-related columns: {client_related_cols}")
            
            # Show first few rows
            print(f"  First few rows:")
            print(df.head(2).to_string())
            print()
            
        except Exception as e:
            print(f"  Error reading {excel_file}: {e}")
else:
    print(f"❌ Extracted directory {extracted_dir} not found") 