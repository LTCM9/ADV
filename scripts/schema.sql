-- Database schema for SEC Investment Adviser Data and Risk Scoring System

-- Create the main filing table
CREATE TABLE IF NOT EXISTS ia_filing (
    id SERIAL PRIMARY KEY,
    sec_number VARCHAR(20) NOT NULL, -- SEC# (e.g., 801-57838)
    crd_number VARCHAR(20), -- Organization CRD# (keeping for reference)
    filing_date DATE NOT NULL,
    firm_name VARCHAR(500), -- Primary Business Name
    legal_name VARCHAR(500),
    sec_region VARCHAR(10), -- SEC Region (e.g., NYRO, SFRO)
    sec_status VARCHAR(50), -- SEC Current Status
    sec_status_date DATE, -- SEC Status Effective Date
    
    -- Assets and Management
    raum NUMERIC(20,2), -- Regulatory Assets Under Management (5A)
    client_count INTEGER, -- Number of clients (5C)
    account_count INTEGER, -- Number of accounts (5D)
    
    -- Compliance
    cco_name VARCHAR(200), -- Chief Compliance Officer Name
    cco_phone VARCHAR(50), -- Chief Compliance Officer Telephone
    cco_email VARCHAR(200), -- Chief Compliance Officer E-mail
    
    -- Business Information
    firm_type VARCHAR(100), -- Firm Type
    umbrella_registration BOOLEAN, -- Umbrella Registration
    website VARCHAR(500), -- Website Address
    
    -- Location
    main_office_city VARCHAR(100),
    main_office_state VARCHAR(10),
    main_office_country VARCHAR(50),
    
    -- Disciplinary Information (Section 11)
    disciplinary_disclosures INTEGER DEFAULT 0, -- Count of Section 11 disclosures
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sec_number, filing_date)
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_ia_filing_sec_date ON ia_filing(sec_number, filing_date);
CREATE INDEX IF NOT EXISTS idx_ia_filing_date ON ia_filing(filing_date);
CREATE INDEX IF NOT EXISTS idx_ia_filing_raum ON ia_filing(raum);

-- Create the change tracking table for risk calculation
CREATE TABLE IF NOT EXISTS ia_change (
    id SERIAL PRIMARY KEY,
    sec_number VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    
    -- Change metrics (compared to previous filing)
    raum_drop_pct NUMERIC(5,2), -- Percentage drop in RAUM
    client_drop_pct NUMERIC(5,2), -- Percentage drop in client count
    acct_drop_pct NUMERIC(5,2), -- Percentage drop in account count
    
    -- Risk flags
    new_disc_flag BOOLEAN DEFAULT FALSE, -- New disciplinary disclosure
    cco_changed BOOLEAN DEFAULT FALSE, -- CCO changed since last filing
    trend_down_flag BOOLEAN DEFAULT FALSE, -- Long-term AUM trend down
    
    -- Additional risk factors
    owner_moves_12m INTEGER DEFAULT 0, -- Number of director/owner moves in 12 months
    adviser_age_years INTEGER, -- Age of the adviser firm
    raum NUMERIC(20,2), -- Current RAUM for size-based risk
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sec_number, filing_date)
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_ia_change_sec_date ON ia_change(sec_number, filing_date);
CREATE INDEX IF NOT EXISTS idx_ia_change_date ON ia_change(filing_date);

-- Create the risk score table
CREATE TABLE IF NOT EXISTS ia_risk_score (
    id SERIAL PRIMARY KEY,
    sec_number VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    score INTEGER NOT NULL, -- Risk score (0-100+)
    factors JSONB, -- JSON object with contributing factors
    risk_category VARCHAR(20) NOT NULL, -- Low, Medium, High, Critical
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sec_number, filing_date)
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_ia_risk_score_sec_date ON ia_risk_score(sec_number, filing_date);
CREATE INDEX IF NOT EXISTS idx_ia_risk_score_category ON ia_risk_score(risk_category);
CREATE INDEX IF NOT EXISTS idx_ia_risk_score_score ON ia_risk_score(score);

-- Create a table for tracking data pipeline runs
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_type VARCHAR(50) NOT NULL, -- 'fetch', 'unzip', 'load', 'risk_calc'
    status VARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed'
    files_processed INTEGER DEFAULT 0,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for pipeline tracking
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_type_date ON pipeline_runs(run_type, started_at);

-- Create a view for dashboard statistics
CREATE OR REPLACE VIEW dashboard_stats AS
SELECT 
    COUNT(DISTINCT sec_number) as total_firms,
    COUNT(*) as total_filings,
    MAX(filing_date) as latest_filing_date,
    AVG(raum) as avg_raum,
    SUM(CASE WHEN raum >= 1000000000 THEN 1 ELSE 0 END) as firms_over_1b,
    SUM(CASE WHEN raum >= 100000000 THEN 1 ELSE 0 END) as firms_over_100m,
    SUM(CASE WHEN raum < 100000000 THEN 1 ELSE 0 END) as firms_under_100m
FROM ia_filing;

-- Create a view for risk statistics
CREATE OR REPLACE VIEW risk_stats AS
SELECT 
    risk_category,
    COUNT(*) as firm_count,
    AVG(score) as avg_score,
    MIN(score) as min_score,
    MAX(score) as max_score
FROM ia_risk_score 
WHERE filing_date = (SELECT MAX(filing_date) FROM ia_risk_score)
GROUP BY risk_category
ORDER BY 
    CASE risk_category 
        WHEN 'Critical' THEN 1 
        WHEN 'High' THEN 2 
        WHEN 'Medium' THEN 3 
        WHEN 'Low' THEN 4 
    END; 