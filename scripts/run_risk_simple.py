#!/usr/bin/env python3
"""
Simple risk calculation script for local Docker setup
"""
import sqlalchemy as sa
from sqlalchemy import text

def main():
    # Use local Docker database connection
    DB_URL = "postgresql://postgres:password@localhost:5432/adv_db"
    
    print("Connecting to database...")
    engine = sa.create_engine(DB_URL)
    
    try:
        with engine.connect() as conn:
            print("✅ Database connection successful")
            
            # Check if we have data
            result = conn.execute(text("SELECT COUNT(*) FROM ia_filing"))
            filing_count = result.scalar()
            print(f"📊 Found {filing_count} filing records")
            
            if filing_count == 0:
                print("❌ No data to calculate risk scores for")
                return
            
            # Check if risk procedure exists
            result = conn.execute(text("""
                SELECT routine_name FROM information_schema.routines 
                WHERE routine_name = 'calc_risk_scores'
            """))
            
            if not result.fetchone():
                print("❌ Risk calculation procedure not found")
                print("Please run the SQL procedure script first")
                return
            
            print("🚀 Executing risk calculation...")
            
            # Execute the risk calculation
            conn.execute(text("CALL calc_risk_scores()"))
            conn.commit()
            
            print("✅ Risk calculation completed!")
            
            # Get results
            result = conn.execute(text("SELECT COUNT(*) FROM ia_risk_score"))
            risk_count = result.scalar()
            print(f"📈 Calculated risk scores for {risk_count} firms")
            
            # Show risk distribution
            result = conn.execute(text("""
                SELECT risk_category, COUNT(*) as count, AVG(score) as avg_score
                FROM ia_risk_score 
                GROUP BY risk_category 
                ORDER BY 
                    CASE risk_category 
                        WHEN 'Critical' THEN 1 
                        WHEN 'High' THEN 2 
                        WHEN 'Medium' THEN 3 
                        WHEN 'Low' THEN 4 
                    END
            """))
            
            print("\n📊 Risk Distribution:")
            for row in result:
                print(f"  {row.risk_category}: {row.count} firms (avg score: {row.avg_score:.1f})")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 