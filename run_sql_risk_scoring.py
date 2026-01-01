#!/usr/bin/env python3
import sqlalchemy as sa
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
connect_args = {"sslmode": "disable"}

engine = sa.create_engine(dsn, connect_args=connect_args)

def run_sql_file(filename):
    """Run a SQL file and return the results"""
    print(f"📄 Running {filename}...")
    
    try:
        with open(filename, 'r') as file:
            sql_content = file.read()
        
        with engine.connect() as conn:
            result = conn.execute(sa.text(sql_content))
            conn.commit()
            print(f"✅ {filename} completed successfully")
            return True
            
    except Exception as e:
        print(f"❌ Error running {filename}: {e}")
        return False

def main():
    print("🚀 Starting SQL-based Risk Scoring Process")
    print("=" * 60)
    
    # Step 1: Populate ia_change table
    print("\n1️⃣ Step 1: Populating ia_change table...")
    success1 = run_sql_file('scripts/populate_ia_change.sql')
    
    if not success1:
        print("❌ Failed to populate ia_change table. Stopping.")
        return
    
    # Step 2: Create/update risk scoring procedure
    print("\n2️⃣ Step 2: Setting up risk scoring procedure...")
    success2 = run_sql_file('scripts/risk_score_procedure_fixed.sql')
    
    if not success2:
        print("❌ Failed to set up risk scoring procedure. Stopping.")
        return
    
    # Step 3: Run the risk scoring procedure
    print("\n3️⃣ Step 3: Calculating risk scores...")
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text("CALL calc_risk_scores()"))
            conn.commit()
            print("✅ Risk scores calculated successfully")
    except Exception as e:
        print(f"❌ Error calculating risk scores: {e}")
        return
    
    # Step 4: Show results
    print("\n4️⃣ Step 4: Risk Scoring Results")
    print("=" * 60)
    
    try:
        with engine.connect() as conn:
            # Get risk statistics
            result = conn.execute(sa.text("SELECT * FROM get_risk_statistics()"))
            print("\n📊 Risk Score Distribution:")
            for row in result:
                category, count, percentage, avg_score = row
                print(f"  {category}: {count:,} firms ({percentage}%) - Avg Score: {avg_score}")
            
            # Get top 10 highest risk firms
            result = conn.execute(sa.text("SELECT * FROM get_firms_by_risk_category('Critical', 10)"))
            print(f"\n🚨 Top 10 Critical Risk Firms:")
            for row in result:
                sec_number, firm_name, score, category, filing_date, factors = row
                print(f"  {firm_name or 'Unknown'}: {score} points ({category})")
            
            # Get total counts
            result = conn.execute(sa.text("SELECT COUNT(*) FROM ia_risk_score"))
            total_risk_scores = result.fetchone()[0]
            print(f"\n📈 Total risk scores calculated: {total_risk_scores:,}")
            
    except Exception as e:
        print(f"❌ Error getting results: {e}")
    
    print("\n🎉 SQL-based risk scoring completed!")

if __name__ == "__main__":
    main() 