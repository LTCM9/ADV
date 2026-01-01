#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check risk scores
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_risk_score"))
        risk_count = result.fetchone()[0]
        print(f"Risk scores calculated: {risk_count:,}")
        
        # Check risk score distribution
        result = conn.execute(sa.text("""
            SELECT 
                risk_category,
                COUNT(*) as count,
                AVG(score) as avg_score,
                MIN(score) as min_score,
                MAX(score) as max_score
            FROM ia_risk_score 
            GROUP BY risk_category 
            ORDER BY avg_score DESC
        """))
        
        print(f"\nRisk Score Distribution:")
        for row in result:
            category, count, avg_score, min_score, max_score = row
            print(f"  {category}: {count:,} firms (avg: {avg_score:.1f}, range: {min_score}-{max_score})")
        
        # Check top 10 highest risk firms
        result = conn.execute(sa.text("""
            SELECT 
                rs.sec_number,
                f.firm_name,
                rs.score,
                rs.risk_category,
                f.raum
            FROM ia_risk_score rs
            LEFT JOIN ia_filing f ON rs.sec_number = f.sec_number AND rs.filing_date = f.filing_date
            ORDER BY rs.score DESC
            LIMIT 10
        """))
        
        print(f"\nTop 10 Highest Risk Firms:")
        for row in result:
            firm_name = row[1] if row[1] else "Unknown"
            raum_str = f"${row[4]:,.0f}" if row[4] else "N/A"
            print(f"  {firm_name}: {row[2]} ({row[3]}) - RAUM: {raum_str}")
        
        # Check firms with disciplinary disclosures
        result = conn.execute(sa.text("""
            SELECT 
                f.firm_name,
                f.raum,
                f.disciplinary_disclosures,
                rs.score,
                rs.risk_category
            FROM ia_filing f
            JOIN ia_risk_score rs ON f.sec_number = rs.sec_number AND f.filing_date = rs.filing_date
            WHERE f.disciplinary_disclosures > 0
            ORDER BY f.disciplinary_disclosures DESC, rs.score DESC
            LIMIT 10
        """))
        
        print(f"\nTop 10 Firms with Disciplinary Disclosures:")
        for row in result:
            firm_name = row[0] if row[0] else "Unknown"
            raum_str = f"${row[1]:,.0f}" if row[1] else "N/A"
            print(f"  {firm_name}: {row[2]} disclosures, Risk: {row[3]} ({row[4]}) - RAUM: {raum_str}")

except Exception as e:
    print(f"❌ Error: {e}") 