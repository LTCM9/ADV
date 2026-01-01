#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check ia_change table
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_change"))
        ia_change_count = result.fetchone()[0]
        print(f"📊 ia_change records: {ia_change_count}")
        
        # Check ia_risk_score table
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_risk_score"))
        ia_risk_score_count = result.fetchone()[0]
        print(f"📊 ia_risk_score records: {ia_risk_score_count}")
        
        # Check ia_filing table
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_filing"))
        ia_filing_count = result.fetchone()[0]
        print(f"📊 ia_filing records: {ia_filing_count}")
        
        # Check if ia_change has data
        if ia_change_count > 0:
            print("\n🔍 Sample ia_change data:")
            result = conn.execute(sa.text("SELECT * FROM ia_change LIMIT 3"))
            for row in result:
                print(f"  {row}")
        else:
            print("\n⚠️  ia_change table is empty - need to populate it for SQL risk scoring")
            
        # Check if ia_risk_score has data
        if ia_risk_score_count > 0:
            print("\n🔍 Sample ia_risk_score data:")
            result = conn.execute(sa.text("SELECT * FROM ia_risk_score LIMIT 3"))
            for row in result:
                print(f"  {row}")
        else:
            print("\n⚠️  ia_risk_score table is empty - need to calculate risk scores")

except Exception as e:
    print(f"❌ Error: {e}") 