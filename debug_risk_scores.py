#!/usr/bin/env python3
import pandas as pd
import sqlalchemy as sa
import numpy as np

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Load the data
        print("🔍 Loading data from ia_filing...")
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
            WHERE sec_number IS NOT NULL
            LIMIT 1000
        """, conn)
        
        print(f"📊 Loaded {len(df)} records")
        print(f"📋 Sample data:")
        print(df.head())
        
        # Check for disciplinary data
        print(f"\n🔍 Disciplinary data analysis:")
        print(f"   Total records: {len(df)}")
        print(f"   Records with disciplinary_disclosures > 0: {len(df[df['disciplinary_disclosures'] > 0])}")
        print(f"   Records with disciplinary_disclosures = 0: {len(df[df['disciplinary_disclosures'] == 0])}")
        print(f"   Records with disciplinary_disclosures is null: {len(df[df['disciplinary_disclosures'].isna()])}")
        
        # Check RAUM data
        print(f"\n💰 RAUM data analysis:")
        print(f"   Records with RAUM > 0: {len(df[df['raum'] > 0])}")
        print(f"   RAUM range: {df['raum'].min()} to {df['raum'].max()}")
        print(f"   RAUM median: {df['raum'].median()}")
        
        # Check client data
        print(f"\n👥 Client data analysis:")
        print(f"   Records with client_count > 0: {len(df[df['client_count'] > 0])}")
        print(f"   Client count range: {df['client_count'].min()} to {df['client_count'].max()}")
        
        # Test risk calculations manually
        print(f"\n🧮 Testing risk calculations:")
        
        # Disciplinary risk
        disciplinary_risk = df['disciplinary_disclosures'].fillna(0) * 10
        print(f"   Disciplinary risk range: {disciplinary_risk.min()} to {disciplinary_risk.max()}")
        print(f"   Disciplinary risk > 0: {len(disciplinary_risk[disciplinary_risk > 0])}")
        
        # Size risk (based on RAUM)
        size_risk = np.where(df['raum'] > 1000000000, 5,  # > $1B
                    np.where(df['raum'] > 100000000, 3,   # > $100M
                    np.where(df['raum'] > 10000000, 1,    # > $10M
                    0)))
        print(f"   Size risk range: {size_risk.min()} to {size_risk.max()}")
        print(f"   Size risk > 0: {len(size_risk[size_risk > 0])}")
        
        # Overall risk (simple test)
        overall_risk = disciplinary_risk + size_risk
        print(f"   Overall risk range: {overall_risk.min()} to {overall_risk.max()}")
        print(f"   Overall risk > 0: {len(overall_risk[overall_risk > 0])}")
        
        # Show some examples
        print(f"\n📋 Sample firms with risk > 0:")
        high_risk = df[overall_risk > 0].head(5)
        for _, row in high_risk.iterrows():
            print(f"   {row['firm_name']}: RAUM=${row['raum']:,.0f}, Disclosures={row['disciplinary_disclosures']}, Risk={overall_risk[df['sec_number'] == row['sec_number']].iloc[0]:.1f}")

except Exception as e:
    print(f"❌ Error: {e}") 