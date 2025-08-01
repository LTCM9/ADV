#!/usr/bin/env python3
"""
run_risk_calculation.py – Execute risk score calculation using SQL procedure.

This script provides a Python interface to execute the SQL-based risk calculation
procedure, combining the performance benefits of SQL with Python's flexibility
for orchestration, logging, and error handling.

Usage
-----
    python3 run_risk_calculation.py [--dry-run] [--verbose]

Dependencies
------------
    pip install sqlalchemy psycopg2-binary python-dotenv
"""
import argparse
import os
import sys
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import text

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_database_connection() -> sa.Engine:
    """Create database connection with proper error handling."""
    required_vars = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    for var in required_vars:
        if not os.getenv(var):
            sys.exit(f"Missing environment variable: {var}")
    
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT", "5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    
    dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return sa.create_engine(dsn, pool_pre_ping=True)

def execute_risk_calculation(engine: sa.Engine, dry_run: bool = False) -> dict:
    """Execute the risk score calculation procedure."""
    start_time = datetime.now()
    
    try:
        with engine.begin() as conn:
            if dry_run:
                print("DRY RUN: Would execute risk calculation procedure")
                return {"status": "dry_run", "message": "No actual calculation performed"}
            
            print("Executing risk score calculation procedure...")
            
            # Execute the SQL procedure
            result = conn.execute(text("CALL calc_risk_scores()"))
            
            # Get statistics after calculation
            stats_result = conn.execute(text("SELECT * FROM get_risk_statistics()"))
            stats = [dict(row) for row in stats_result]
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "success",
                "duration_seconds": duration,
                "statistics": stats,
                "message": f"Risk calculation completed in {duration:.2f} seconds"
            }
            
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "status": "error",
            "duration_seconds": duration,
            "error": str(e),
            "message": f"Risk calculation failed after {duration:.2f} seconds: {str(e)}"
        }

def get_risk_summary(engine: sa.Engine) -> dict:
    """Get a summary of current risk scores."""
    try:
        with engine.connect() as conn:
            # Get total count
            count_result = conn.execute(text("SELECT COUNT(*) as total FROM ia_risk_score"))
            total_count = count_result.scalar()
            
            # Get latest calculation date
            date_result = conn.execute(text("SELECT MAX(updated_at) as latest FROM ia_risk_score"))
            latest_date = date_result.scalar()
            
            # Get risk distribution
            stats_result = conn.execute(text("SELECT * FROM get_risk_statistics()"))
            stats = [dict(row) for row in stats_result]
            
            return {
                "total_firms": total_count,
                "latest_calculation": latest_date,
                "risk_distribution": stats
            }
            
    except Exception as e:
        return {"error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Execute risk score calculation using SQL procedure")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be executed without running")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    parser.add_argument("--summary", action="store_true", help="Show current risk score summary only")
    args = parser.parse_args()
    
    print("Connecting to database...")
    engine = get_database_connection()
    
    if args.summary:
        print("Getting current risk score summary...")
        summary = get_risk_summary(engine)
        
        if "error" in summary:
            print(f"Error getting summary: {summary['error']}")
            sys.exit(1)
        
        print(f"\nRisk Score Summary:")
        print(f"Total firms with risk scores: {summary['total_firms']}")
        print(f"Latest calculation: {summary['latest_calculation']}")
        print(f"\nRisk Distribution:")
        
        for stat in summary['risk_distribution']:
            print(f"  {stat['risk_category']}: {stat['firm_count']} firms ({stat['percentage']}%) - Avg Score: {stat['avg_score']}")
        
        return
    
    # Execute risk calculation
    result = execute_risk_calculation(engine, args.dry_run)
    
    if result["status"] == "success":
        print(f"✅ {result['message']}")
        
        if args.verbose and "statistics" in result:
            print(f"\nRisk Score Statistics:")
            for stat in result["statistics"]:
                print(f"  {stat['risk_category']}: {stat['firm_count']} firms ({stat['percentage']}%) - Avg Score: {stat['avg_score']}")
    
    elif result["status"] == "error":
        print(f"❌ {result['message']}")
        sys.exit(1)
    
    elif result["status"] == "dry_run":
        print(f"ℹ️  {result['message']}")

if __name__ == "__main__":
    main() 