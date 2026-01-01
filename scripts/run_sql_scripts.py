#!/usr/bin/env python3
"""
run_sql_scripts.py - Execute SQL procedures for risk scoring
"""
import os
import sys
import sqlalchemy as sa
from pathlib import Path

def get_dsn_and_engine():
    """Get database connection string and engine"""
    for var in ("PGHOST","PGDATABASE","PGUSER","PGPASSWORD"):
        if not os.getenv(var):
            sys.exit(f"Missing environment variable: {var}")
    
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT","5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    
    dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    # Use SSL only for remote connections, disable for local and Docker
    ssl_mode = "disable" if host in ["localhost", "127.0.0.1", "postgres"] else "require"
    engine = sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": ssl_mode})
    return dsn, engine

def read_sql_file(file_path):
    """Read SQL file content"""
    with open(file_path, 'r') as f:
        return f.read()

def execute_sql_script(engine, script_name, sql_content):
    """Execute SQL script and handle errors"""
    try:
        print(f"📋 Executing {script_name}...")
        with engine.begin() as conn:
            conn.execute(sa.text(sql_content))
        print(f"✅ {script_name} completed successfully")
        return True
    except Exception as e:
        print(f"❌ Error executing {script_name}: {e}")
        return False

def main():
    """Main execution function"""
    print("🚀 Starting SQL procedure execution...")
    
    # Get database connection
    dsn, engine = get_dsn_and_engine()
    print(f"🔗 Connected to database: {dsn.split('@')[1] if '@' in dsn else 'localhost'}")
    
    # Define SQL scripts to execute
    scripts_dir = Path(__file__).parent
    sql_scripts = [
        ("populate_ia_change.sql", scripts_dir / "populate_ia_change.sql"),
        ("risk_score_procedure_fixed.sql", scripts_dir / "risk_score_procedure_fixed.sql"),
    ]
    
    # Execute each script
    success_count = 0
    for script_name, script_path in sql_scripts:
        if not script_path.exists():
            print(f"⚠️  Script not found: {script_path}")
            continue
            
        sql_content = read_sql_file(script_path)
        if execute_sql_script(engine, script_name, sql_content):
            success_count += 1
    
    print(f"\n📊 Summary: {success_count}/{len(sql_scripts)} scripts executed successfully")
    
    if success_count == len(sql_scripts):
        print("🎉 All SQL procedures completed successfully!")
        
        # Run the risk calculation procedure
        print("\n🔢 Running risk score calculation...")
        try:
            with engine.begin() as conn:
                conn.execute(sa.text("CALL calc_risk_scores();"))
            print("✅ Risk scores calculated successfully!")
        except Exception as e:
            print(f"❌ Error calculating risk scores: {e}")
    else:
        print("⚠️  Some scripts failed. Please check the errors above.")

if __name__ == "__main__":
    main() 