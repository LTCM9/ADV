#!/usr/bin/env python3
import os
import pandas as pd
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check total records
        result = conn.execute(sa.text("SELECT COUNT(*) as total_records FROM ia_filing"))
        total = result.fetchone()[0]
        print(f"✅ Total records loaded: {total:,}")
        
        # Check by filing date to see distribution
        result = conn.execute(sa.text("""
            SELECT filing_date, COUNT(*) as count 
            FROM ia_filing 
            GROUP BY filing_date 
            ORDER BY filing_date DESC
        """))
        dates = result.fetchall()
        print(f"\n📅 Records by filing date (last 10):")
        for date, count in dates[:10]:
            print(f"  {date}: {count:,} records")
        
        # Check unique SEC numbers
        result = conn.execute(sa.text("SELECT COUNT(DISTINCT sec_number) as unique_firms FROM ia_filing"))
        unique_firms = result.fetchone()[0]
        print(f"\n🏢 Unique firms (by SEC#): {unique_firms:,}")
        
        # Check for any NULL SEC numbers
        result = conn.execute(sa.text("SELECT COUNT(*) as null_sec FROM ia_filing WHERE sec_number IS NULL"))
        null_sec = result.fetchone()[0]
        print(f"❌ Records with NULL SEC numbers: {null_sec}")
        
        # Check file count vs expected
        print(f"\n📁 Expected files: 66 (18 CSV + 48 Excel)")
        print(f"📁 Actual files: {len(os.listdir('data/unzipped/iapd'))}")
        
        # Check if we have data from all years
        result = conn.execute(sa.text("""
            SELECT EXTRACT(YEAR FROM filing_date) as year, COUNT(*) as count 
            FROM ia_filing 
            GROUP BY EXTRACT(YEAR FROM filing_date) 
            ORDER BY year DESC
        """))
        years = result.fetchall()
        print(f"\n📊 Records by year:")
        for year, count in years:
            print(f"  {int(year)}: {count:,} records")

except Exception as e:
    print(f"❌ Error: {e}") 