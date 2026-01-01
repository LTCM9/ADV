#!/usr/bin/env python3
import sqlalchemy as sa
import json

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check what's actually in the factors field
        result = conn.execute(sa.text("""
            SELECT 
                rs.sec_number,
                f.firm_name,
                rs.score,
                rs.risk_category,
                rs.factors,
                f.raum,
                f.disciplinary_disclosures
            FROM ia_risk_score rs
            LEFT JOIN ia_filing f ON rs.sec_number = f.sec_number AND rs.filing_date = f.filing_date
            ORDER BY rs.score DESC
            LIMIT 10
        """))
        
        print("🔍 Debugging Risk Factors Storage:")
        print("=" * 60)
        
        for i, row in enumerate(result, 1):
            firm_name = row[1] if row[1] else "Unknown"
            score = row[2]
            category = row[3]
            factors_raw = row[4]
            raum = row[5]
            disciplinary = row[6]
            
            print(f"\n{i}. {firm_name}")
            print(f"   Score: {score} ({category})")
            print(f"   RAUM: ${raum:,.0f}" if raum else "   RAUM: N/A")
            print(f"   Disciplinary: {disciplinary}")
            print(f"   Factors (raw): {factors_raw}")
            
            # Try to parse factors
            try:
                if factors_raw:
                    factors = json.loads(factors_raw)
                    print(f"   Factors (parsed): {factors}")
                else:
                    print(f"   Factors (parsed): None/Empty")
            except Exception as e:
                print(f"   Factors (parse error): {e}")
        
        # Check if factors are being stored at all
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN factors IS NULL OR factors = '{}' THEN 1 END) as empty_factors,
                COUNT(CASE WHEN factors IS NOT NULL AND factors != '{}' THEN 1 END) as has_factors
            FROM ia_risk_score
        """))
        
        stats = result.fetchone()
        print(f"\n" + "=" * 60)
        print(f"📊 Factors Storage Statistics:")
        print(f"   Total records: {stats[0]:,}")
        print(f"   Empty factors: {stats[1]:,}")
        print(f"   Has factors: {stats[2]:,}")
        
        # Look at a few non-empty factors
        result = conn.execute(sa.text("""
            SELECT factors FROM ia_risk_score 
            WHERE factors IS NOT NULL AND factors != '{}'
            LIMIT 5
        """))
        
        print(f"\nSample Non-Empty Factors:")
        for i, row in enumerate(result, 1):
            print(f"   {i}. {row[0]}")

except Exception as e:
    print(f"❌ Error: {e}") 