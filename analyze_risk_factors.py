#!/usr/bin/env python3
import sqlalchemy as sa
import json

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Get a sample of risk scores with their factors
        result = conn.execute(sa.text("""
            SELECT 
                rs.sec_number,
                f.firm_name,
                rs.score,
                rs.risk_category,
                rs.factors,
                f.raum,
                f.client_count,
                f.account_count,
                f.disciplinary_disclosures
            FROM ia_risk_score rs
            LEFT JOIN ia_filing f ON rs.sec_number = rs.sec_number AND rs.filing_date = f.filing_date
            ORDER BY rs.score DESC
            LIMIT 20
        """))
        
        print("🔍 Analyzing Risk Factors in Top 20 Highest Risk Firms:")
        print("=" * 80)
        
        for i, row in enumerate(result, 1):
            firm_name = row[1] if row[1] else "Unknown"
            score = row[2]
            category = row[3]
            factors_json = row[4]
            raum = row[5]
            client_count = row[6]
            account_count = row[7]
            disciplinary = row[8]
            
            # Parse the factors JSON
            try:
                factors = json.loads(factors_json) if factors_json else {}
            except:
                factors = {}
            
            print(f"\n{i}. {firm_name}")
            print(f"   Overall Score: {score} ({category})")
            print(f"   RAUM: ${raum:,.0f}" if raum else "   RAUM: N/A")
            print(f"   Clients: {client_count:,}" if client_count else "   Clients: N/A")
            print(f"   Accounts: {account_count:,}" if account_count else "   Accounts: N/A")
            print(f"   Disciplinary: {disciplinary}" if disciplinary else "   Disciplinary: 0")
            print(f"   Risk Factors Breakdown:")
            
            for factor, value in factors.items():
                print(f"     - {factor}: {value}")
        
        # Analyze factor distribution across all firms
        print(f"\n" + "=" * 80)
        print("📊 Risk Factor Analysis Across All Firms:")
        
        # Check which factors are contributing to scores
        result = conn.execute(sa.text("""
            SELECT factors FROM ia_risk_score LIMIT 1000
        """))
        
        factor_counts = {}
        factor_values = {}
        
        for row in result:
            try:
                factors = json.loads(row[0]) if row[0] else {}
                for factor, value in factors.items():
                    if factor not in factor_counts:
                        factor_counts[factor] = 0
                        factor_values[factor] = []
                    factor_counts[factor] += 1
                    factor_values[factor].append(value)
            except:
                continue
        
        print(f"\nFactor Contribution Analysis:")
        for factor, count in factor_counts.items():
            avg_value = sum(factor_values[factor]) / len(factor_values[factor]) if factor_values[factor] else 0
            max_value = max(factor_values[factor]) if factor_values[factor] else 0
            print(f"  {factor}:")
            print(f"    - Firms with this factor: {count:,}")
            print(f"    - Average value: {avg_value:.1f}")
            print(f"    - Maximum value: {max_value}")
        
        # Check what factors are missing (always 0)
        print(f"\nFactors That Are NOT Working (Always 0):")
        for factor, values in factor_values.items():
            if all(v == 0 for v in values):
                print(f"  ❌ {factor}: Always 0 (not contributing to risk scores)")
            else:
                print(f"  ✅ {factor}: Contributing to risk scores")

except Exception as e:
    print(f"❌ Error: {e}") 