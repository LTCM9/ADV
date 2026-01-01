#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Clear the risk score table
        conn.execute(sa.text("TRUNCATE TABLE ia_risk_score"))
        conn.commit()
        print("✅ Risk score table cleared")
        
        # Check if table is empty
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_risk_score"))
        count = result.fetchone()[0]
        print(f"📊 Records in risk score table: {count}")

except Exception as e:
    print(f"❌ Error: {e}") 