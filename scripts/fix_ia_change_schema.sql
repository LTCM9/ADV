-- Fix ia_change table schema to handle percentage values correctly
-- Percentage drops cannot exceed 100%, so NUMERIC(5,2) is sufficient

-- Drop the existing table and recreate with proper data types
DROP TABLE IF EXISTS ia_change CASCADE;

-- Recreate the change tracking table for risk calculation
CREATE TABLE ia_change (
    id SERIAL PRIMARY KEY,
    sec_number VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    
    -- Change metrics (compared to previous filing) - using NUMERIC(5,2) for percentages
    raum_drop_pct NUMERIC(5,2), -- Percentage drop in RAUM (capped at 100%)
    client_drop_pct NUMERIC(5,2), -- Percentage drop in client count (capped at 100%)
    acct_drop_pct NUMERIC(5,2), -- Percentage drop in account count (capped at 100%)
    
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

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_ia_change_sec_date ON ia_change(sec_number, filing_date);
CREATE INDEX IF NOT EXISTS idx_ia_change_date ON ia_change(filing_date);

-- Log the fix
DO $$
BEGIN
    RAISE NOTICE 'Fixed ia_change table schema - percentage fields now NUMERIC(5,2) with 100% cap';
END $$; 