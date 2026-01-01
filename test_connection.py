#!/usr/bin/env python3
import sqlalchemy as sa

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_filing"))
        count = result.fetchone()[0]
        print(f"✅ Connected! Found {count:,} records in ia_filing")
        
        # Test ia_change table
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_change"))
        count = result.fetchone()[0]
        print(f"📊 ia_change table has {count:,} records")
        
except Exception as e:
    print(f"❌ Connection error: {e}") 