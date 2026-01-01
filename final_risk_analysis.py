#!/usr/bin/env python3
import sqlalchemy as sa
import ast

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
                f.disciplinary_disclosures
            FROM ia_risk_score rs
            LEFT JOIN ia_filing f ON rs.sec_number = f.sec_number AND rs.filing_date = f.filing_date
            ORDER BY rs.score DESC
            LIMIT 10
        """))
        
        print("🔍 Risk Factor Analysis - Top 10 Highest Risk Firms:")
        print("=" * 80)
        
        for i, row in enumerate(result, 1):
            firm_name = row[1] if row[1] else "Unknown"
            score = row[2]
            category = row[3]
            factors_raw = row[4]
            raum = row[5]
            client_count = row[6]
            disciplinary = row[7]
            
            # Parse the factors (it's stored as a dict, not JSON string)
            try:
                factors = ast.literal_eval(str(factors_raw)) if factors_raw else {}
            except:
                factors = {}
            
            print(f"\n{i}. {firm_name}")
            print(f"   Overall Score: {score} ({category})")
            print(f"   RAUM: ${raum:,.0f}" if raum else "   RAUM: N/A")
            print(f"   Clients: {client_count:,}" if client_count else "   Clients: N/A")
            print(f"   Disciplinary: {disciplinary}")
            print(f"   Risk Factors Breakdown:")
            
            for factor, value in factors.items():
                status = "✅" if value > 0 else "❌"
                print(f"     {status} {factor}: {value}")
        
        # Analyze factor distribution across all firms
        print(f"\n" + "=" * 80)
        print("📊 Risk Factor Analysis Across All Firms:")
        
        # Get a sample of factors to analyze
        result = conn.execute(sa.text("""
            SELECT factors FROM ia_risk_score LIMIT 1000
        """))
        
        factor_stats = {}
        
        for row in result:
            try:
                factors = ast.literal_eval(str(row[0])) if row[0] else {}
                for factor, value in factors.items():
                    if factor not in factor_stats:
                        factor_stats[factor] = {
                            'count': 0,
                            'total_value': 0,
                            'max_value': 0,
                            'non_zero_count': 0
                        }
                    factor_stats[factor]['count'] += 1
                    factor_stats[factor]['total_value'] += value
                    factor_stats[factor]['max_value'] = max(factor_stats[factor]['max_value'], value)
                    if value > 0:
                        factor_stats[factor]['non_zero_count'] += 1
            except:
                continue
        
        print(f"\nFactor Contribution Analysis:")
        for factor, stats in factor_stats.items():
            avg_value = stats['total_value'] / stats['count'] if stats['count'] > 0 else 0
            non_zero_pct = (stats['non_zero_count'] / stats['count']) * 100 if stats['count'] > 0 else 0
            
            status = "✅ WORKING" if stats['non_zero_count'] > 0 else "❌ NOT WORKING"
            print(f"\n  {factor}:")
            print(f"    Status: {status}")
            print(f"    Firms with this factor: {stats['count']:,}")
            print(f"    Firms with non-zero values: {stats['non_zero_count']:,} ({non_zero_pct:.1f}%)")
            print(f"    Average value: {avg_value:.1f}")
            print(f"    Maximum value: {stats['max_value']}")
        
        # Summary of working vs non-working factors
        print(f"\n" + "=" * 80)
        print("🎯 SUMMARY: Which Risk Factors Are Working")
        print("=" * 80)
        
        working_factors = []
        non_working_factors = []
        
        for factor, stats in factor_stats.items():
            if stats['non_zero_count'] > 0:
                working_factors.append(factor)
            else:
                non_working_factors.append(factor)
        
        print(f"\n✅ WORKING FACTORS ({len(working_factors)}):")
        for factor in working_factors:
            stats = factor_stats[factor]
            non_zero_pct = (stats['non_zero_count'] / stats['count']) * 100
            print(f"  • {factor}: {stats['non_zero_count']:,} firms ({non_zero_pct:.1f}%)")
        
        print(f"\n❌ NON-WORKING FACTORS ({len(non_working_factors)}):")
        for factor in non_working_factors:
            print(f"  • {factor}: Always 0 (no historical data available)")
        
        print(f"\n💡 EXPLANATION:")
        print(f"  • Working factors have actual data to calculate from")
        print(f"  • Non-working factors need historical data (filing history, CCO changes, etc.)")
        print(f"  • These can be enhanced when we have more historical data")

except Exception as e:
    print(f"❌ Error: {e}") 