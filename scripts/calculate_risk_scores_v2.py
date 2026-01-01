#!/usr/bin/env python3
"""
Calculate risk scores for investment advisers based on various factors.
This script loads data from ia_filing table and calculates risk scores.
"""

import os
import sys
import json
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
dsn = f"postgresql+psycopg2://{os.getenv('PGUSER', 'iapdadmin')}:{os.getenv('PGPASSWORD', 'AdvPwd#2025')}@{os.getenv('PGHOST', '127.0.0.1')}:{os.getenv('PGPORT', '5432')}/{os.getenv('PGDATABASE', 'iapd')}"

# Disable SSL for local connections
connect_args = {"sslmode": "disable"} if os.getenv('PGHOST', '127.0.0.1') in ['127.0.0.1', 'localhost'] else {}

engine = sa.create_engine(dsn, connect_args=connect_args)

def calculate_risk_score(row):
    """Calculate overall risk score based on various factors."""
    score = 0
    factors = {}
    
    # 1. Disciplinary Risk (0-25 points)
    if hasattr(row, 'disciplinary_disclosures') and row.disciplinary_disclosures and row.disciplinary_disclosures > 0:
        disciplinary_score = min(25, row.disciplinary_disclosures * 10)
        score += disciplinary_score
        factors['disciplinary_risk'] = disciplinary_score
    
    # 2. Size Factor Risk (0-20 points)
    if hasattr(row, 'raum') and row.raum:
        if row.raum > 10000000000:  # > $10B
            size_score = 5
        elif row.raum > 1000000000:  # > $1B
            size_score = 10
        elif row.raum > 100000000:  # > $100M
            size_score = 15
        else:
            size_score = 20
        score += size_score
        factors['size_factor_risk'] = size_score
    
    # 3. Client Concentration Risk (0-15 points)
    if hasattr(row, 'client_count') and hasattr(row, 'raum') and row.client_count and row.raum:
        avg_client_size = row.raum / row.client_count
        if avg_client_size > 10000000:  # > $10M per client
            concentration_score = 15
        elif avg_client_size > 1000000:  # > $1M per client
            concentration_score = 10
        elif avg_client_size > 100000:  # > $100K per client
            concentration_score = 5
        else:
            concentration_score = 0
        score += concentration_score
        factors['client_concentration_risk'] = concentration_score
    
    # 4. Filing Compliance Risk (0-15 points)
    # For now, assume all firms are compliant since we don't have filing history
    filing_score = 0
    score += filing_score
    factors['filing_compliance_risk'] = filing_score
    
    # 5. CCO Stability Risk (0-15 points)
    # For now, assume stable since we don't have CCO change history
    cco_score = 0
    score += cco_score
    factors['cco_stability_risk'] = cco_score
    
    # 6. AUM Volatility Risk (0-10 points)
    # For now, assume stable since we don't have historical AUM data
    volatility_score = 0
    score += volatility_score
    factors['aum_volatility_risk'] = volatility_score
    
    # Determine risk category
    if score >= 50:
        risk_category = 'Critical'
    elif score >= 30:
        risk_category = 'High'
    elif score >= 15:
        risk_category = 'Medium'
    else:
        risk_category = 'Low'
    
    return score, risk_category, factors

def main():
    """Main function to calculate and store risk scores."""
    print("🔍 Starting risk score calculation...")
    
    try:
        with engine.connect() as conn:
            # Get ALL firms with RAUM > 0 (not just distinct ones)
            query = text("""
                SELECT 
                    sec_number,
                    filing_date,
                    firm_name,
                    raum,
                    client_count,
                    account_count,
                    disciplinary_disclosures
                FROM ia_filing 
                WHERE raum > 0
                ORDER BY sec_number, filing_date DESC
            """)
            
            result = conn.execute(query)
            firms = result.fetchall()
            
            print(f"📊 Processing {len(firms)} firms...")
            
            # Calculate risk scores
            risk_scores = []
            for i, firm in enumerate(firms):
                if i % 10000 == 0:
                    print(f"   Processed {i}/{len(firms)} firms...")
                
                score, category, factors = calculate_risk_score(firm)
                
                risk_scores.append({
                    'sec_number': firm.sec_number,
                    'filing_date': firm.filing_date,
                    'score': score,
                    'risk_category': category,
                    'factors': json.dumps(factors),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
            
            print(f"✅ Calculated risk scores for {len(risk_scores)} firms")
            
            # Clear existing risk scores
            conn.execute(text("TRUNCATE TABLE ia_risk_score"))
            print("🗑️  Cleared existing risk scores")
            
            # Insert new risk scores
            if risk_scores:
                insert_query = text("""
                    INSERT INTO ia_risk_score 
                    (sec_number, filing_date, score, risk_category, factors, created_at, updated_at)
                    VALUES (:sec_number, :filing_date, :score, :risk_category, :factors, :created_at, :updated_at)
                """)
                
                conn.execute(insert_query, risk_scores)
                conn.commit()
                print(f"💾 Saved {len(risk_scores)} risk scores to database")
            
            # Print summary statistics
            summary_query = text("""
                SELECT 
                    risk_category,
                    COUNT(*) as count,
                    AVG(score) as avg_score,
                    MIN(score) as min_score,
                    MAX(score) as max_score
                FROM ia_risk_score 
                GROUP BY risk_category 
                ORDER BY avg_score DESC
            """)
            
            summary_result = conn.execute(summary_query)
            print("\n📈 Risk Score Summary:")
            for row in summary_result:
                print(f"  {row.risk_category}: {row.count:,} firms (avg: {row.avg_score:.1f}, range: {row.min_score}-{row.max_score})")
            
            # Show top 10 highest risk firms
            top_risk_query = text("""
                SELECT 
                    rs.sec_number,
                    f.firm_name,
                    rs.score,
                    rs.risk_category,
                    f.raum
                FROM ia_risk_score rs
                LEFT JOIN ia_filing f ON rs.sec_number = f.sec_number AND rs.filing_date = f.filing_date
                ORDER BY rs.score DESC
                LIMIT 10
            """)
            
            top_result = conn.execute(top_risk_query)
            print(f"\n🚨 Top 10 Highest Risk Firms:")
            for row in top_result:
                raum_str = f"${row.raum:,.0f}" if row.raum else "N/A"
                print(f"  {row.firm_name or 'Unknown'}: {row.score} ({row.risk_category}) - RAUM: {raum_str}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 