#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        print("🔍 Debugging DISTINCT ON Query")
        print("=" * 60)
        
        # 1. Check total unique SEC numbers
        result = conn.execute(sa.text("""
            SELECT COUNT(DISTINCT sec_number) as unique_sec_numbers
            FROM ia_filing
        """))
        
        unique_sec = result.fetchone()[0]
        print(f"1. Total unique SEC numbers: {unique_sec:,}")
        
        # 2. Check unique SEC numbers with RAUM > 0
        result = conn.execute(sa.text("""
            SELECT COUNT(DISTINCT sec_number) as unique_sec_with_raum
            FROM ia_filing
            WHERE raum > 0
        """))
        
        unique_sec_with_raum = result.fetchone()[0]
        print(f"2. Unique SEC numbers with RAUM > 0: {unique_sec_with_raum:,}")
        
        # 3. Check what the DISTINCT ON query actually returns
        result = conn.execute(sa.text("""
            SELECT COUNT(*) as distinct_on_count
            FROM (
                SELECT DISTINCT ON (sec_number) 
                    sec_number,
                    filing_date,
                    firm_name,
                    raum,
                    client_count,
                    account_count,
                    disciplinary_disclosures
                FROM ia_filing 
                WHERE raum > 0
                ORDER BY sec_number, filing_date DESC
            ) latest_filings
        """))
        
        distinct_on_count = result.fetchone()[0]
        print(f"3. DISTINCT ON query result count: {distinct_on_count:,}")
        
        # 4. Check if there are duplicate SEC numbers
        result = conn.execute(sa.text("""
            SELECT 
                sec_number,
                COUNT(*) as filing_count
            FROM ia_filing
            WHERE raum > 0
            GROUP BY sec_number
            HAVING COUNT(*) > 1
            ORDER BY filing_count DESC
            LIMIT 10
        """))
        
        print(f"\n4. SEC numbers with multiple filings (top 10):")
        for row in result:
            sec_number, count = row
            print(f"   {sec_number}: {count} filings")
        
        # 5. Check filing date distribution
        result = conn.execute(sa.text("""
            SELECT 
                filing_date,
                COUNT(*) as count
            FROM ia_filing
            WHERE raum > 0
            GROUP BY filing_date
            ORDER BY filing_date DESC
            LIMIT 10
        """))
        
        print(f"\n5. Filing dates (most recent 10):")
        for row in result:
            filing_date, count = row
            print(f"   {filing_date}: {count:,} filings")
        
        # 6. Check if the issue is with the ORDER BY clause
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_filings,
                COUNT(DISTINCT sec_number) as unique_firms,
                COUNT(DISTINCT filing_date) as unique_dates
            FROM ia_filing
            WHERE raum > 0
        """))
        
        stats = result.fetchone()
        print(f"\n6. Filing statistics:")
        print(f"   Total filings with RAUM > 0: {stats[0]:,}")
        print(f"   Unique firms: {stats[1]:,}")
        print(f"   Unique filing dates: {stats[2]:,}")
        
        # 7. Check if there are NULL values causing issues
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as null_sec_numbers,
                COUNT(*) as null_filing_dates
            FROM ia_filing
            WHERE raum > 0 AND (sec_number IS NULL OR filing_date IS NULL)
        """))
        
        null_stats = result.fetchone()
        print(f"\n7. NULL value check:")
        print(f"   Records with NULL sec_number or filing_date: {null_stats[0]:,}")

except Exception as e:
    print(f"❌ Error: {e}") 