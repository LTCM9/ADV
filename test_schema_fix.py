#!/usr/bin/env python3
"""
Test script to verify the SQLAlchemy schema fix works
"""
import os
import pandas as pd
import sqlalchemy as sa
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed")

def test_connection():
    """Test database connection and to_sql method"""
    try:
        for var in ("PGHOST","PGDATABASE","PGUSER","PGPASSWORD"):
            if not os.getenv(var):
                print(f"Missing environment variable: {var}")
                return False
        
        host = os.environ["PGHOST"]
        port = os.getenv("PGPORT","5432")
        user = os.environ["PGUSER"]
        pwd = os.environ["PGPASSWORD"]
        db = os.environ["PGDATABASE"]
        dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        
        # Use SSL only for remote connections (RDS), disable for local
        ssl_mode = "require" if host != "localhost" else "disable"
        engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": ssl_mode})
        
        test_df = pd.DataFrame({
            'crd': [12345],
            'filing_date': ['2024-01-01'],
            'raum': [1000000],
            'total_clients': [100],
            'total_accounts': [150],
            'cco_id': ['TEST|USER|123'],
            'disclosure_flag': ['N']
        })
        
        with engine.connect() as conn:
            test_df.to_sql("ia_filing_test", conn, if_exists="replace", index=False, schema=None)
        
        print("✓ Schema fix test passed - no parameter conflicts")
        
        with engine.connect() as conn:
            conn.execute(sa.text("DROP TABLE IF EXISTS ia_filing_test"))
        
        return True
        
    except Exception as e:
        print(f"✗ Schema fix test failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing SQLAlchemy schema fix...")
    success = test_connection()
    if success:
        print("Schema fix is working correctly!")
    else:
        print("Schema fix needs more work.")
