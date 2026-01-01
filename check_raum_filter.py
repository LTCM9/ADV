#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check RAUM distribution
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_firms,
                COUNT(CASE WHEN raum > 0 THEN 1 END) as firms_with_raum,
                COUNT(CASE WHEN raum = 0 THEN 1 END) as firms_raum_zero,
                COUNT(CASE WHEN raum IS NULL THEN 1 END) as firms_raum_null
            FROM ia_filing
        """))
        
        stats = result.fetchone()
        print("🔍 RAUM Data Investigation:")
        print("=" * 60)
        print(f"Total firms: {stats[0]:,}")
        print(f"Firms with RAUM > 0: {stats[1]:,}")
        print(f"Firms with RAUM = 0: {stats[2]:,}")
        print(f"Firms with RAUM = NULL: {stats[3]:,}")
        
        # Check disciplinary disclosures by RAUM status
        result = conn.execute(sa.text("""
            SELECT 
                CASE 
                    WHEN raum > 0 THEN 'RAUM > 0'
                    WHEN raum = 0 THEN 'RAUM = 0'
                    WHEN raum IS NULL THEN 'RAUM = NULL'
                END as raum_status,
                COUNT(*) as total_firms,
                COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) as firms_with_disclosures,
                ROUND(COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as disclosure_pct
            FROM ia_filing
            GROUP BY 
                CASE 
                    WHEN raum > 0 THEN 'RAUM > 0'
                    WHEN raum = 0 THEN 'RAUM = 0'
                    WHEN raum IS NULL THEN 'RAUM = NULL'
                END
            ORDER BY raum_status
        """))
        
        print(f"\n📊 Disciplinary Disclosures by RAUM Status:")
        for row in result:
            status, total, disclosures, pct = row
            print(f"  {status}: {total:,} firms, {disclosures:,} with disclosures ({pct}%)")
        
        # Check what the risk scoring script is actually processing
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as firms_processed,
                COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) as with_disclosures,
                ROUND(COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as disclosure_pct
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
        
        stats2 = result.fetchone()
        print(f"\n🔍 Risk Scoring Script Processing:")
        print(f"Firms processed (RAUM > 0): {stats2[0]:,}")
        print(f"Firms with disclosures in processed set: {stats2[1]:,}")
        print(f"Disclosure percentage in processed set: {stats2[2]}%")
        
        # Check what we're missing
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as missing_firms,
                COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) as missing_with_disclosures
            FROM ia_filing 
            WHERE raum = 0 OR raum IS NULL
        """))
        
        stats3 = result.fetchone()
        print(f"\n❌ What We're Missing:")
        print(f"Firms excluded (RAUM = 0 or NULL): {stats3[0]:,}")
        print(f"Firms with disclosures that are excluded: {stats3[1]:,}")
        
        # Show some examples of firms with disclosures but no RAUM
        result = conn.execute(sa.text("""
            SELECT 
                firm_name,
                sec_number,
                raum,
                disciplinary_disclosures,
                filing_date
            FROM ia_filing 
            WHERE disciplinary_disclosures > 0 AND (raum = 0 OR raum IS NULL)
            ORDER BY filing_date DESC
            LIMIT 10
        """))
        
        print(f"\n🚨 Examples of Firms with Disclosures but No RAUM:")
        for row in result:
            firm_name = row[0] if row[0] else "Unknown"
            raum = row[2] if row[2] is not None else "NULL"
            print(f"  {firm_name}: {row[3]} disclosures, RAUM: {raum}")

except Exception as e:
    print(f"❌ Error: {e}") 