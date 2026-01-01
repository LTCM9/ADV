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
                AVG(overall_risk_score) as avg_score,
                MIN(overall_risk_score) as min_score,
                MAX(overall_risk_score) as max_score
            FROM ia_risk_score 
            GROUP BY risk_category 
            ORDER BY avg_score DESC
        """))
        
        print(f"\nRisk Score Distribution:")
        for row in result:
            category, count, avg_score, min_score, max_score = row
            print(f"  {category}: {count:,} firms (avg: {avg_score:.2f}, range: {min_score:.2f}-{max_score:.2f})")
        
        # Check top 10 highest risk firms
        result = conn.execute(sa.text("""
            SELECT 
                rs.firm_name,
                rs.overall_risk_score,
                rs.risk_category,
                rs.disciplinary_risk,
                rs.size_factor_risk,
                f.raum
            FROM ia_risk_score rs
            LEFT JOIN ia_filing f ON rs.sec_number = f.sec_number
            ORDER BY rs.overall_risk_score DESC
            LIMIT 10
        """))
        
        print(f"\nTop 10 Highest Risk Firms:")
        for row in result:
            firm_name, risk_score, category, disciplinary, size_risk, raum = row
            raum_str = f"${raum:,.0f}" if raum else "N/A"
            print(f"  {firm_name}: {risk_score:.2f} ({category}) - Disciplinary: {disciplinary:.2f}, Size: {size_risk:.2f}, RAUM: {raum_str}")

except Exception as e:
    print(f"❌ Error: {e}") 