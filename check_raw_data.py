#!/usr/bin/env python3
import pandas as pd
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check a few sample records with their raw values
        print("🔍 Examining raw data values...")
        df = pd.read_sql("""
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
            LIMIT 10
        """, conn)
        
        print(f"📊 Top 10 firms by RAUM:")
        for _, row in df.iterrows():
            print(f"   {row['firm_name']}: RAUM=${row['raum']:,.2f}, Clients={row['client_count']}, Disclosures={row['disciplinary_disclosures']}")
        
        # Check firms with disciplinary disclosures
        print(f"\n🔍 Firms with disciplinary disclosures:")
        df_disciplinary = pd.read_sql("""
            SELECT 
                sec_number,
                firm_name,
                raum,
                client_count,
                disciplinary_disclosures,
                filing_date
            FROM ia_filing 
            WHERE disciplinary_disclosures > 0
            ORDER BY disciplinary_disclosures DESC
            LIMIT 10
        """, conn)
        
        if len(df_disciplinary) > 0:
            for _, row in df_disciplinary.iterrows():
                print(f"   {row['firm_name']}: Disclosures={row['disciplinary_disclosures']}, RAUM=${row['raum']:,.2f}")
        else:
            print("   No firms found with disciplinary disclosures > 0")
        
        # Check RAUM distribution
        print(f"\n💰 RAUM distribution:")
        raum_stats = pd.read_sql("""
            SELECT 
                COUNT(*) as total_firms,
                COUNT(CASE WHEN raum > 0 THEN 1 END) as firms_with_raum,
                MIN(raum) as min_raum,
                MAX(raum) as max_raum,
                AVG(raum) as avg_raum,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY raum) as median_raum,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY raum) as p95_raum
            FROM ia_filing
        """, conn)
        
        stats = raum_stats.iloc[0]
        print(f"   Total firms: {stats['total_firms']:,}")
        print(f"   Firms with RAUM > 0: {stats['firms_with_raum']:,}")
        print(f"   RAUM range: ${stats['min_raum']:,.2f} to ${stats['max_raum']:,.2f}")
        print(f"   RAUM average: ${stats['avg_raum']:,.2f}")
        print(f"   RAUM median: ${stats['median_raum']:,.2f}")
        print(f"   RAUM 95th percentile: ${stats['p95_raum']:,.2f}")

except Exception as e:
    print(f"❌ Error: {e}") 