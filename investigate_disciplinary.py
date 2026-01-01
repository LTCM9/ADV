#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check disciplinary data in the main table
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_firms,
                COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) as firms_with_disclosures,
                COUNT(CASE WHEN disciplinary_disclosures = 0 THEN 1 END) as firms_without_disclosures,
                COUNT(CASE WHEN disciplinary_disclosures IS NULL THEN 1 END) as firms_null_disclosures,
                SUM(disciplinary_disclosures) as total_disclosures
            FROM ia_filing
        """))
        
        stats = result.fetchone()
        print("🔍 Disciplinary Data Investigation:")
        print("=" * 60)
        print(f"Total firms in ia_filing: {stats[0]:,}")
        print(f"Firms with disciplinary disclosures > 0: {stats[1]:,}")
        print(f"Firms with disciplinary disclosures = 0: {stats[2]:,}")
        print(f"Firms with disciplinary disclosures = NULL: {stats[3]:,}")
        print(f"Total disciplinary disclosures: {stats[4]:,}")
        
        # Check the distribution of disciplinary values
        result = conn.execute(sa.text("""
            SELECT 
                disciplinary_disclosures,
                COUNT(*) as count
            FROM ia_filing 
            GROUP BY disciplinary_disclosures 
            ORDER BY disciplinary_disclosures
        """))
        
        print(f"\n📊 Disciplinary Disclosures Distribution:")
        for row in result:
            value = row[0] if row[0] is not None else "NULL"
            count = row[1]
            print(f"  {value}: {count:,} firms")
        
        # Look at some firms with disciplinary disclosures
        result = conn.execute(sa.text("""
            SELECT 
                firm_name,
                sec_number,
                disciplinary_disclosures,
                filing_date
            FROM ia_filing 
            WHERE disciplinary_disclosures > 0
            ORDER BY disciplinary_disclosures DESC
            LIMIT 10
        """))
        
        print(f"\n🚨 Top 10 Firms with Disciplinary Disclosures:")
        for row in result:
            firm_name = row[0] if row[0] else "Unknown"
            print(f"  {firm_name}: {row[2]} disclosures (SEC#: {row[1]}, Date: {row[3]})")
        
        # Check if there's a mismatch between ia_filing and ia_risk_score
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_risk_scores,
                COUNT(CASE WHEN rs.factors::text LIKE '%disciplinary_risk%' THEN 1 END) as with_disciplinary_factor
            FROM ia_risk_score rs
        """))
        
        stats2 = result.fetchone()
        print(f"\n🔍 Risk Score vs Filing Data:")
        print(f"Total risk scores: {stats2[0]:,}")
        print(f"Risk scores with disciplinary factor: {stats2[1]:,}")
        
        # Check a few specific firms to see what's happening
        result = conn.execute(sa.text("""
            SELECT 
                f.firm_name,
                f.sec_number,
                f.disciplinary_disclosures,
                rs.score,
                rs.factors
            FROM ia_filing f
            LEFT JOIN ia_risk_score rs ON f.sec_number = rs.sec_number AND f.filing_date = rs.filing_date
            WHERE f.disciplinary_disclosures > 0
            ORDER BY f.disciplinary_disclosures DESC
            LIMIT 5
        """))
        
        print(f"\n🔍 Sample Firms - Filing vs Risk Score:")
        for row in result:
            firm_name = row[0] if row[0] else "Unknown"
            filing_disclosures = row[2]
            risk_score = row[3] if row[3] else "N/A"
            factors = row[4] if row[4] else "N/A"
            print(f"  {firm_name}:")
            print(f"    Filing disclosures: {filing_disclosures}")
            print(f"    Risk score: {risk_score}")
            print(f"    Factors: {factors}")

except Exception as e:
    print(f"❌ Error: {e}") 