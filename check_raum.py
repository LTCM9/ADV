#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Get top 10 firms by RAUM
        result = conn.execute(sa.text("""
            SELECT firm_name, raum 
            FROM ia_filing 
            WHERE raum > 0 
            ORDER BY raum DESC 
            LIMIT 10
        """))
        
        print("Top 10 firms by RAUM:")
        for row in result:
            firm_name = row[0] if row[0] else "Unknown"
            raum = row[1] if row[1] else 0
            print(f"  {firm_name}: ${raum:,.0f}")
        
        # Get overall RAUM statistics
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_firms,
                COUNT(CASE WHEN raum > 0 THEN 1 END) as firms_with_raum,
                MIN(raum) as min_raum,
                MAX(raum) as max_raum,
                AVG(raum) as avg_raum,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY raum) as p95_raum,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY raum) as p99_raum
            FROM ia_filing
        """))
        
        stats = result.fetchone()
        print(f"\nRAUM Statistics:")
        print(f"  Total firms: {stats[0]:,}")
        print(f"  Firms with RAUM > 0: {stats[1]:,}")
        print(f"  RAUM range: ${stats[2]:,.0f} to ${stats[3]:,.0f}")
        print(f"  RAUM average: ${stats[4]:,.0f}")
        print(f"  RAUM 95th percentile: ${stats[5]:,.0f}")
        print(f"  RAUM 99th percentile: ${stats[6]:,.0f}")

except Exception as e:
    print(f"❌ Error: {e}") 