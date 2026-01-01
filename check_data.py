#!/usr/bin/env python3
import os
import pandas as pd
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    # Check total records
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT COUNT(*) as total_records FROM ia_filing"))
        total = result.fetchone()[0]
        print(f"✅ Total records loaded: {total:,}")
        
        # Check date range
        result = conn.execute(sa.text("SELECT MIN(filing_date), MAX(filing_date) FROM ia_filing"))
        min_date, max_date = result.fetchone()
        print(f"📅 Date range: {min_date} to {max_date}")
        
        # Check unique firms
        result = conn.execute(sa.text("SELECT COUNT(DISTINCT sec_number) as unique_firms FROM ia_filing"))
        unique_firms = result.fetchone()[0]
        print(f"🏢 Unique firms: {unique_firms:,}")
        
        # Sample some data
        result = conn.execute(sa.text("SELECT sec_number, firm_name, raum, filing_date FROM ia_filing LIMIT 5"))
        print("\n📊 Sample data:")
        for row in result:
            print(f"  {row[0]} | {row[1][:30]}... | ${row[2]:,.0f}M | {row[3]}")

except Exception as e:
    print(f"❌ Error: {e}") 