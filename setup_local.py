#!/usr/bin/env python3
"""
Local development setup script for IAPD Risk Scoring System
Checks PostgreSQL connection, creates database if needed, and initializes schema
"""
import os
import sys
import subprocess
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed")

import sqlalchemy as sa
from sqlalchemy import text

def check_postgres_running():
    """Check if PostgreSQL service is running"""
    try:
        result = subprocess.run(['sudo', 'systemctl', 'is-active', 'postgresql'], 
                              capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip() == 'active':
            print("✓ PostgreSQL service is running")
            return True
        else:
            print("✗ PostgreSQL service is not running")
            print("  Run: sudo systemctl start postgresql")
            return False
    except Exception as e:
        print(f"✗ Error checking PostgreSQL status: {e}")
        return False

def get_db_config():
    """Get database configuration from environment"""
    config = {
        'host': os.getenv('PGHOST', 'localhost'),
        'port': os.getenv('PGPORT', '5432'),
        'database': os.getenv('PGDATABASE', 'iapd'),
        'user': os.getenv('PGUSER', 'iapdadmin'),
        'password': os.getenv('PGPASSWORD')
    }
    
    if not config['password']:
        print("✗ PGPASSWORD not set in environment")
        print("  Check your .env file")
        return None
    
    return config

def test_connection(config):
    """Test database connection"""
    try:
        dsn = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": "disable"})
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print(f"✓ Successfully connected to database '{config['database']}'")
        return engine
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return None

def check_schema(engine):
    """Check if schema tables exist"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name IN ('ia_filing', 'ia_risk_score', 'pipeline_runs')
            """))
            tables = [row[0] for row in result]
            
            if len(tables) == 3:
                print("✓ Database schema is initialized")
                return True
            else:
                print(f"✗ Missing tables. Found: {tables}")
                return False
    except Exception as e:
        print(f"✗ Error checking schema: {e}")
        return False

def initialize_schema(engine):
    """Initialize database schema"""
    try:
        schema_file = Path("scripts/schema.sql")
        if not schema_file.exists():
            print("✗ Schema file not found: scripts/schema.sql")
            return False
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(schema_sql))
        
        print("✓ Database schema initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Error initializing schema: {e}")
        return False

def main():
    """Main setup function"""
    print("IAPD Risk Scoring System - Local Setup")
    print("=" * 50)
    
    if not check_postgres_running():
        sys.exit(1)
    
    config = get_db_config()
    if not config:
        sys.exit(1)
    
    print(f"Database config: {config['user']}@{config['host']}:{config['port']}/{config['database']}")
    
    engine = test_connection(config)
    if not engine:
        print("\nTroubleshooting:")
        print("1. Make sure PostgreSQL is running: sudo systemctl start postgresql")
        print("2. Check if user exists: sudo -u postgres psql -c '\\du'")
        print("3. Check if database exists: sudo -u postgres psql -c '\\l'")
        print("4. Verify .env file has correct PGPASSWORD")
        sys.exit(1)
    
    if not check_schema(engine):
        print("Initializing database schema...")
        if not initialize_schema(engine):
            sys.exit(1)
    
    print("\n✓ Local development environment is ready!")
    print("\nNext steps:")
    print("1. Run data fetching: python scripts/fetch_iapd_data.py")
    print("2. Extract data: python scripts/unzip_iapd_zips.py")
    print("3. Load to database: python load_data.py data/unzipped/iapd/")
    print("4. Calculate risk scores: python scripts/run_risk_calculation.py")
    print("5. Start API: cd api && python app.py")

if __name__ == "__main__":
    main()
