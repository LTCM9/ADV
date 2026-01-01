#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        print("🔍 Understanding the Scoring Metrics")
        print("=" * 60)
        
        # 1. Check what the actual disciplinary data looks like
        print("\n1. DISCIPLINARY DATA ANALYSIS:")
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_firms,
                COUNT(CASE WHEN disciplinary_disclosures > 0 THEN 1 END) as with_disclosures,
                COUNT(CASE WHEN disciplinary_disclosures = 0 THEN 1 END) as zero_disclosures,
                COUNT(CASE WHEN disciplinary_disclosures IS NULL THEN 1 END) as null_disclosures,
                SUM(disciplinary_disclosures) as total_disclosures
            FROM ia_filing
        """))
        
        stats = result.fetchone()
        print(f"   Total firms: {stats[0]:,}")
        print(f"   Firms with disclosures > 0: {stats[1]:,}")
        print(f"   Firms with disclosures = 0: {stats[2]:,}")
        print(f"   Firms with disclosures = NULL: {stats[3]:,}")
        print(f"   Total disclosure count: {stats[4]:,}")
        
        # 2. Check the distribution of disciplinary values
        print(f"\n2. DISCIPLINARY VALUE DISTRIBUTION:")
        result = conn.execute(sa.text("""
            SELECT 
                disciplinary_disclosures,
                COUNT(*) as count
            FROM ia_filing 
            GROUP BY disciplinary_disclosures 
            ORDER BY disciplinary_disclosures
        """))
        
        for row in result:
            value = row[0] if row[0] is not None else "NULL"
            count = row[1]
            print(f"   {value}: {count:,} firms")
        
        # 3. Check what the risk scoring script is actually doing
        print(f"\n3. RISK SCORING SCRIPT ANALYSIS:")
        
        # Check how many firms the script should process
        result = conn.execute(sa.text("""
            SELECT COUNT(*) as firms_with_raum
            FROM ia_filing 
            WHERE raum > 0
        """))
        
        firms_with_raum = result.fetchone()[0]
        print(f"   Firms with RAUM > 0: {firms_with_raum:,}")
        
        # Check how many of those have disciplinary disclosures
        result = conn.execute(sa.text("""
            SELECT COUNT(*) as firms_with_disclosures
            FROM ia_filing 
            WHERE raum > 0 AND disciplinary_disclosures > 0
        """))
        
        firms_with_disclosures = result.fetchone()[0]
        print(f"   Firms with RAUM > 0 AND disclosures > 0: {firms_with_disclosures:,}")
        
        # 4. Check the current risk scores
        print(f"\n4. CURRENT RISK SCORES:")
        result = conn.execute(sa.text("""
            SELECT COUNT(*) as total_risk_scores
            FROM ia_risk_score
        """))
        
        total_risk_scores = result.fetchone()[0]
        print(f"   Total risk scores calculated: {total_risk_scores:,}")
        
        # Check how many risk scores have disciplinary factors
        result = conn.execute(sa.text("""
            SELECT COUNT(*) as with_disciplinary
            FROM ia_risk_score 
            WHERE factors::text LIKE '%disciplinary_risk%'
        """))
        
        with_disciplinary = result.fetchone()[0]
        print(f"   Risk scores with disciplinary factor: {with_disciplinary:,}")
        
        # 5. Check the scoring logic
        print(f"\n5. SCORING LOGIC ANALYSIS:")
        print(f"   The script should process: {firms_with_raum:,} firms")
        print(f"   Of those, {firms_with_disclosures:,} should have disciplinary risk")
        print(f"   But we only have: {with_disciplinary:,} firms with disciplinary risk in risk scores")
        print(f"   Missing: {firms_with_disclosures - with_disciplinary:,} firms with disciplinary disclosures")
        
        # 6. Check a few specific examples
        print(f"\n6. SPECIFIC EXAMPLES:")
        result = conn.execute(sa.text("""
            SELECT 
                f.firm_name,
                f.sec_number,
                f.raum,
                f.disciplinary_disclosures,
                rs.score,
                rs.factors
            FROM ia_filing f
            LEFT JOIN ia_risk_score rs ON f.sec_number = rs.sec_number AND f.filing_date = rs.filing_date
            WHERE f.disciplinary_disclosures > 0
            ORDER BY f.disciplinary_disclosures DESC
            LIMIT 5
        """))
        
        for row in result:
            firm_name = row[0] if row[0] else "Unknown"
            raum = row[2] if row[2] else "N/A"
            disclosures = row[3]
            risk_score = row[4] if row[4] else "N/A"
            factors = row[5] if row[5] else "N/A"
            
            print(f"   {firm_name}:")
            print(f"     RAUM: {raum}")
            print(f"     Disclosures: {disclosures}")
            print(f"     Risk Score: {risk_score}")
            print(f"     Factors: {factors}")
            print()

except Exception as e:
    print(f"❌ Error: {e}") 