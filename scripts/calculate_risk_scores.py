#!/usr/bin/env python3
"""
calculate_risk_scores.py – Calculate risk scores for investment advisers based on SEC data.

This script analyzes the ia_filing table and calculates comprehensive risk scores based on:
- Regulatory disclosures and disciplinary history
- Assets under management trends
- Client concentration
- Compliance officer changes
- Filing frequency and completeness

Usage
-----
    python3 calculate_risk_scores.py [--update-existing] [--dry-run]

Dependencies
------------
    pip install pandas sqlalchemy psycopg2-binary python-dotenv numpy scipy
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Risk scoring weights and thresholds
RISK_WEIGHTS = {
    'disclosure_history': 0.35,      # 35% - Regulatory issues
    'aum_volatility': 0.20,          # 20% - Asset management stability
    'client_concentration': 0.15,    # 15% - Client diversification
    'filing_compliance': 0.15,       # 15% - Regulatory filing behavior
    'cco_stability': 0.10,           # 10% - Compliance officer stability
    'size_factor': 0.05              # 5% - Firm size considerations
}

# Risk thresholds
RISK_THRESHOLDS = {
    'low': 0.0,
    'medium': 0.3,
    'high': 0.6,
    'critical': 0.8
}

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
    return sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": "require"})

def create_risk_scores_table(engine: sa.Engine) -> None:
    """Create the risk_scores table if it doesn't exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS risk_scores (
        crd INTEGER PRIMARY KEY,
        firm_name TEXT,
        overall_risk_score DECIMAL(5,4),
        risk_category VARCHAR(20),
        disclosure_risk DECIMAL(5,4),
        aum_volatility_risk DECIMAL(5,4),
        client_concentration_risk DECIMAL(5,4),
        filing_compliance_risk DECIMAL(5,4),
        cco_stability_risk DECIMAL(5,4),
        size_factor_risk DECIMAL(5,4),
        last_calculation_date TIMESTAMP,
        risk_factors JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_risk_scores_category ON risk_scores(risk_category);
    CREATE INDEX IF NOT EXISTS idx_risk_scores_score ON risk_scores(overall_risk_score);
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_sql))

def calculate_disclosure_risk(df: pd.DataFrame) -> Tuple[pd.Series, Dict]:
    """Calculate risk based on regulatory disclosures."""
    # Group by CRD to get disclosure history
    disclosure_data = df.groupby('crd').agg({
        'disclosure_flag': lambda x: (x == 'Y').sum(),
        'filing_date': ['min', 'max', 'count']
    }).reset_index()
    
    disclosure_data.columns = ['crd', 'disclosure_count', 'first_filing', 'last_filing', 'filing_count']
    
    # Calculate disclosure rate (disclosures per year of operation)
    disclosure_data['years_active'] = (
        pd.to_datetime(disclosure_data['last_filing']) - 
        pd.to_datetime(disclosure_data['first_filing'])
    ).dt.days / 365.25
    
    disclosure_data['disclosure_rate'] = (
        disclosure_data['disclosure_count'] / 
        disclosure_data['years_active'].clip(lower=0.1)
    )
    
    # Normalize to 0-1 scale using log transformation
    disclosure_risk = np.log1p(disclosure_data['disclosure_rate']) / np.log(10)
    disclosure_risk = disclosure_risk.clip(0, 1)
    
    risk_factors = {
        'disclosure_count': disclosure_data['disclosure_count'].to_dict(),
        'disclosure_rate': disclosure_data['disclosure_rate'].to_dict(),
        'years_active': disclosure_data['years_active'].to_dict()
    }
    
    return disclosure_risk, risk_factors

def calculate_aum_volatility_risk(df: pd.DataFrame) -> Tuple[pd.Series, Dict]:
    """Calculate risk based on AUM volatility over time."""
    # Get AUM data for each firm over time
    aum_data = df[['crd', 'filing_date', 'raum']].dropna()
    aum_data['filing_date'] = pd.to_datetime(aum_data['filing_date'])
    
    # Calculate AUM volatility for each firm
    aum_volatility = []
    risk_factors = {}
    
    for crd in aum_data['crd'].unique():
        firm_aum = aum_data[aum_data['crd'] == crd].sort_values('filing_date')
        
        if len(firm_aum) < 2:
            aum_volatility.append(0.0)
            risk_factors[crd] = {'aum_volatility': 0.0, 'aum_trend': 0.0}
            continue
        
        # Calculate percentage changes
        firm_aum['aum_change'] = firm_aum['raum'].pct_change()
        
        # Calculate volatility (standard deviation of percentage changes)
        volatility = firm_aum['aum_change'].std()
        
        # Calculate trend (positive = growing, negative = declining)
        trend = firm_aum['aum_change'].mean()
        
        # Normalize volatility to 0-1 scale
        normalized_volatility = min(volatility * 10, 1.0)  # Scale factor of 10
        
        aum_volatility.append(normalized_volatility)
        risk_factors[crd] = {
            'aum_volatility': float(volatility),
            'aum_trend': float(trend),
            'aum_data_points': len(firm_aum)
        }
    
    return pd.Series(aum_volatility, index=aum_data['crd'].unique()), risk_factors

def calculate_client_concentration_risk(df: pd.DataFrame) -> Tuple[pd.Series, Dict]:
    """Calculate risk based on client concentration."""
    # Use total_clients and total_accounts to assess concentration
    client_data = df[['crd', 'total_clients', 'total_accounts']].dropna()
    
    # Calculate client-to-account ratio (higher ratio = more concentrated)
    client_data['client_account_ratio'] = client_data['total_clients'] / client_data['total_accounts'].clip(lower=1)
    
    # Normalize to 0-1 scale (higher ratio = higher risk)
    max_ratio = client_data['client_account_ratio'].quantile(0.95)
    concentration_risk = (client_data['client_account_ratio'] / max_ratio).clip(0, 1)
    
    risk_factors = {
        'client_account_ratio': client_data['client_account_ratio'].to_dict(),
        'total_clients': client_data['total_clients'].to_dict(),
        'total_accounts': client_data['total_accounts'].to_dict()
    }
    
    return concentration_risk, risk_factors

def calculate_filing_compliance_risk(df: pd.DataFrame) -> Tuple[pd.Series, Dict]:
    """Calculate risk based on filing compliance and frequency."""
    # Analyze filing patterns
    filing_data = df.groupby('crd').agg({
        'filing_date': ['min', 'max', 'count'],
        'disclosure_flag': lambda x: (x == 'Y').sum()
    }).reset_index()
    
    filing_data.columns = ['crd', 'first_filing', 'last_filing', 'filing_count', 'disclosure_count']
    
    # Calculate filing frequency and consistency
    filing_data['years_active'] = (
        pd.to_datetime(filing_data['last_filing']) - 
        pd.to_datetime(filing_data['first_filing'])
    ).dt.days / 365.25
    
    filing_data['filing_frequency'] = filing_data['filing_count'] / filing_data['years_active'].clip(lower=0.1)
    
    # Expected filing frequency is typically quarterly (4 per year)
    expected_frequency = 4.0
    filing_data['compliance_ratio'] = filing_data['filing_frequency'] / expected_frequency
    
    # Risk is higher for firms that file less frequently than expected
    compliance_risk = (1 - filing_data['compliance_ratio']).clip(0, 1)
    
    risk_factors = {
        'filing_frequency': filing_data['filing_frequency'].to_dict(),
        'compliance_ratio': filing_data['compliance_ratio'].to_dict(),
        'filing_count': filing_data['filing_count'].to_dict()
    }
    
    return compliance_risk, risk_factors

def calculate_cco_stability_risk(df: pd.DataFrame) -> Tuple[pd.Series, Dict]:
    """Calculate risk based on Chief Compliance Officer stability."""
    # Analyze CCO changes over time
    cco_data = df[['crd', 'filing_date', 'cco_id']].dropna()
    cco_data['filing_date'] = pd.to_datetime(cco_data['filing_date'])
    
    cco_stability_risk = []
    risk_factors = {}
    
    for crd in cco_data['crd'].unique():
        firm_cco = cco_data[cco_data['crd'] == crd].sort_values('filing_date')
        
        if len(firm_cco) < 2:
            cco_stability_risk.append(0.0)
            risk_factors[crd] = {'cco_changes': 0, 'cco_stability_period': 0}
            continue
        
        # Count CCO changes
        cco_changes = (firm_cco['cco_id'] != firm_cco['cco_id'].shift()).sum()
        
        # Calculate average CCO tenure
        years_active = (firm_cco['filing_date'].max() - firm_cco['filing_date'].min()).days / 365.25
        avg_tenure = years_active / max(cco_changes, 1)
        
        # Normalize risk (more changes = higher risk)
        stability_risk = min(cco_changes / 5.0, 1.0)  # Cap at 5 changes
        
        cco_stability_risk.append(stability_risk)
        risk_factors[crd] = {
            'cco_changes': int(cco_changes),
            'avg_tenure_years': float(avg_tenure),
            'years_active': float(years_active)
        }
    
    return pd.Series(cco_stability_risk, index=cco_data['crd'].unique()), risk_factors

def calculate_size_factor_risk(df: pd.DataFrame) -> Tuple[pd.Series, Dict]:
    """Calculate risk based on firm size considerations."""
    # Get latest AUM for each firm
    latest_data = df.sort_values('filing_date').groupby('crd').tail(1)
    
    # Size risk is generally lower for larger firms (more resources, better compliance)
    # But very large firms can have complexity risks
    aum_values = latest_data['raum'].fillna(0)
    
    # U-shaped risk curve: very small and very large firms have higher risk
    # Optimal size is around $1B-$10B
    optimal_size = 5e9  # $5B
    size_deviation = np.abs(aum_values - optimal_size) / optimal_size
    
    # Normalize to 0-1 scale
    size_risk = np.tanh(size_deviation)  # Smooth curve between 0 and 1
    
    risk_factors = {
        'aum': aum_values.to_dict(),
        'size_deviation': size_deviation.to_dict()
    }
    
    return size_risk, risk_factors

def calculate_overall_risk_score(risk_components: Dict[str, pd.Series]) -> pd.Series:
    """Calculate weighted overall risk score."""
    overall_score = pd.Series(0.0, index=risk_components['disclosure_risk'].index)
    
    for component, weight in RISK_WEIGHTS.items():
        if component in risk_components:
            overall_score += risk_components[component] * weight
    
    return overall_score.clip(0, 1)

def categorize_risk(score: float) -> str:
    """Categorize risk score into risk levels."""
    if score >= RISK_THRESHOLDS['critical']:
        return 'Critical'
    elif score >= RISK_THRESHOLDS['high']:
        return 'High'
    elif score >= RISK_THRESHOLDS['medium']:
        return 'Medium'
    else:
        return 'Low'

def main():
    parser = argparse.ArgumentParser(description="Calculate risk scores for investment advisers")
    parser.add_argument("--update-existing", action="store_true", help="Update existing risk scores")
    parser.add_argument("--dry-run", action="store_true", help="Calculate scores without saving to database")
    args = parser.parse_args()
    
    print("Connecting to database...")
    engine = get_database_connection()
    
    print("Creating risk_scores table...")
    create_risk_scores_table(engine)
    
    print("Loading data from ia_filing table...")
    query = """
    SELECT crd, filing_date, raum, total_clients, total_accounts, 
           cco_id, disclosure_flag 
    FROM ia_filing 
    ORDER BY filing_date
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No data found in ia_filing table. Please run the data loading script first.")
        sys.exit(1)
    
    print(f"Processing {len(df)} records for {df['crd'].nunique()} firms...")
    
    # Calculate individual risk components
    print("Calculating disclosure risk...")
    disclosure_risk, disclosure_factors = calculate_disclosure_risk(df)
    
    print("Calculating AUM volatility risk...")
    aum_volatility_risk, aum_factors = calculate_aum_volatility_risk(df)
    
    print("Calculating client concentration risk...")
    client_concentration_risk, client_factors = calculate_client_concentration_risk(df)
    
    print("Calculating filing compliance risk...")
    filing_compliance_risk, filing_factors = calculate_filing_compliance_risk(df)
    
    print("Calculating CCO stability risk...")
    cco_stability_risk, cco_factors = calculate_cco_stability_risk(df)
    
    print("Calculating size factor risk...")
    size_factor_risk, size_factors = calculate_size_factor_risk(df)
    
    # Combine all risk components
    risk_components = {
        'disclosure_risk': disclosure_risk,
        'aum_volatility_risk': aum_volatility_risk,
        'client_concentration_risk': client_concentration_risk,
        'filing_compliance_risk': filing_compliance_risk,
        'cco_stability_risk': cco_stability_risk,
        'size_factor_risk': size_factor_risk
    }
    
    print("Calculating overall risk scores...")
    overall_risk_scores = calculate_overall_risk_score(risk_components)
    
    # Create results DataFrame
    results = []
    for crd in overall_risk_scores.index:
        risk_score = overall_risk_scores[crd]
        risk_category = categorize_risk(risk_score)
        
        # Combine all risk factors for this firm
        all_factors = {}
        for factor_dict in [disclosure_factors, aum_factors, client_factors, 
                           filing_factors, cco_factors, size_factors]:
            if crd in factor_dict:
                all_factors.update(factor_dict[crd])
        
        result = {
            'crd': crd,
            'overall_risk_score': float(risk_score),
            'risk_category': risk_category,
            'disclosure_risk': float(disclosure_risk.get(crd, 0)),
            'aum_volatility_risk': float(aum_volatility_risk.get(crd, 0)),
            'client_concentration_risk': float(client_concentration_risk.get(crd, 0)),
            'filing_compliance_risk': float(filing_compliance_risk.get(crd, 0)),
            'cco_stability_risk': float(cco_stability_risk.get(crd, 0)),
            'size_factor_risk': float(size_factor_risk.get(crd, 0)),
            'risk_factors': all_factors,
            'last_calculation_date': datetime.now()
        }
        results.append(result)
    
    results_df = pd.DataFrame(results)
    
    # Print summary statistics
    print("\nRisk Score Summary:")
    print(f"Total firms processed: {len(results_df)}")
    print(f"Average risk score: {results_df['overall_risk_score'].mean():.3f}")
    print(f"Risk distribution:")
    for category in ['Low', 'Medium', 'High', 'Critical']:
        count = len(results_df[results_df['risk_category'] == category])
        percentage = (count / len(results_df)) * 100
        print(f"  {category}: {count} firms ({percentage:.1f}%)")
    
    if args.dry_run:
        print("\nDry run completed. No data saved to database.")
        return
    
    # Save to database
    print("\nSaving risk scores to database...")
    
    if args.update_existing:
        print("Performing bulk update of existing risk scores...")
        
        update_df = results_df.copy()
        update_df['risk_factors'] = update_df['risk_factors'].apply(json.dumps)
        update_df['updated_at'] = datetime.now()
        
        with engine.begin() as conn:
            temp_table = "temp_risk_scores_update"
            update_df.to_sql(temp_table, conn, if_exists='replace', index=False, method='multi')
            
            upsert_sql = f"""
            INSERT INTO risk_scores (
                crd, overall_risk_score, risk_category, disclosure_risk,
                aum_volatility_risk, client_concentration_risk, filing_compliance_risk,
                cco_stability_risk, size_factor_risk, risk_factors, 
                last_calculation_date, updated_at
            )
            SELECT 
                crd, overall_risk_score, risk_category, disclosure_risk,
                aum_volatility_risk, client_concentration_risk, filing_compliance_risk,
                cco_stability_risk, size_factor_risk, risk_factors::jsonb,
                last_calculation_date, updated_at
            FROM {temp_table}
            ON CONFLICT (crd) DO UPDATE SET
                overall_risk_score = EXCLUDED.overall_risk_score,
                risk_category = EXCLUDED.risk_category,
                disclosure_risk = EXCLUDED.disclosure_risk,
                aum_volatility_risk = EXCLUDED.aum_volatility_risk,
                client_concentration_risk = EXCLUDED.client_concentration_risk,
                filing_compliance_risk = EXCLUDED.filing_compliance_risk,
                cco_stability_risk = EXCLUDED.cco_stability_risk,
                size_factor_risk = EXCLUDED.size_factor_risk,
                risk_factors = EXCLUDED.risk_factors,
                last_calculation_date = EXCLUDED.last_calculation_date,
                updated_at = CURRENT_TIMESTAMP
            """
            
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
            
        print(f"Updated {len(results_df)} risk score records")
    else:
        # Bulk insert new records
        print("Performing bulk insert of new risk scores...")
        
        insert_df = results_df.copy()
        insert_df['risk_factors'] = insert_df['risk_factors'].apply(json.dumps)
        insert_df['created_at'] = datetime.now()
        insert_df['updated_at'] = datetime.now()
        
        insert_df.to_sql('risk_scores', engine, if_exists='append', index=False, method='multi')
        print(f"Inserted {len(results_df)} new risk score records")
    
    print("Risk score calculation completed successfully! ✔")

if __name__ == "__main__":
    main()    