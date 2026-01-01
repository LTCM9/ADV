#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Clear the table
        conn.execute(sa.text("TRUNCATE TABLE ia_filing"))
        conn.commit()
        print("✅ Table cleared successfully")
        
        # Verify it's empty
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_filing"))
        count = result.fetchone()[0]
        print(f"📊 Records in table: {count}")

except Exception as e:
    print(f"❌ Error: {e}") 