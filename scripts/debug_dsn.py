#!/usr/bin/env python3
import os
import sqlalchemy as sa

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Build DSN
host = os.environ.get("PGHOST", "localhost")
port = os.environ.get("PGPORT", "5432")
user = os.environ.get("PGUSER", "postgres")
pwd = os.environ.get("PGPASSWORD", "")
database = os.environ.get("PGDATABASE", "postgres")

dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{database}"

print(f"DSN: {dsn}")
print(f"Host: {host}")
print(f"User: {user}")
print(f"Database: {database}")

# Try to connect with explicit SSL disable
try:
    engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT 1 as test"))
        print("✅ Connection successful!")
        print(f"Test query result: {result.fetchone()}")
except Exception as e:
    print(f"❌ Connection failed: {e}") 