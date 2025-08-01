-- Enhanced Risk Score Calculation Procedure
-- Combines the user's approach with additional risk factors

-- Create the risk scores table if it doesn't exist
CREATE TABLE IF NOT EXISTS ia_risk_score (
    crd INTEGER,
    filing_date DATE,
    score DECIMAL(5,2),
    factors JSONB,
    risk_category VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (crd, filing_date)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_ia_risk_score_crd ON ia_risk_score(crd);
CREATE INDEX IF NOT EXISTS idx_ia_risk_score_category ON ia_risk_score(risk_category);
CREATE INDEX IF NOT EXISTS idx_ia_risk_score_date ON ia_risk_score(filing_date);

-- Enhanced risk score calculation procedure
CREATE OR REPLACE PROCEDURE calc_risk_scores()
LANGUAGE SQL
AS $$
BEGIN
    -- Insert/update risk scores with enhanced calculation
    INSERT INTO ia_risk_score (crd, filing_date, score, factors, risk_category)
    SELECT
        crd,
        filing_date,

        -- Core risk signals
        40  * COALESCE(new_disc_flag, 0)                              -- New disciplinary flag
        + 25  * CASE WHEN raum_drop_pct >= 25 THEN 1 ELSE 0 END       -- AUM drop ≥ 25%
        + 15  * CASE WHEN raum_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END  -- AUM drop 15–24.9%
        + 15  * CASE WHEN client_drop_pct >= 25 THEN 1 ELSE 0 END     -- Client drop ≥ 25%
        +  8  * CASE WHEN client_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END -- Client drop 15–24.9%
        + 10  * CASE WHEN acct_drop_pct >= 25 THEN 1 ELSE 0 END       -- Account drop ≥ 25%
        +  5  * CASE WHEN acct_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END   -- Account drop 15–24.9%
        + 10  * COALESCE(cco_changed, 0)                              -- CCO changed
        + 10  * COALESCE(trend_down_flag, 0)                          -- Long-term AUM trend down

        -- Additional risk factors
        + 10  * CASE WHEN owner_moves_12m >= 2 THEN 1 ELSE 0 END      -- ≥ 2 director/owner moves in 12m
        +  5  * CASE WHEN adviser_age_years < 3 THEN 1 ELSE 0 END     -- Adviser < 3 yrs old
        +  5  * CASE WHEN raum < 50000000 THEN 1 ELSE 0 END           -- RAUM < $50M

        AS score,

        -- Factors JSON for drill-down
        jsonb_build_object(
            'aum_drop_pct',     COALESCE(raum_drop_pct, 0),
            'client_drop_pct',  COALESCE(client_drop_pct, 0),
            'acct_drop_pct',    COALESCE(acct_drop_pct, 0),
            'new_disc',         COALESCE(new_disc_flag, 0),
            'cco_changed',      COALESCE(cco_changed, 0),
            'trend_down',       COALESCE(trend_down_flag, 0),
            'owner_moves_12m',  COALESCE(owner_moves_12m, 0),
            'adviser_age_yrs',  COALESCE(adviser_age_years, 0),
            'small_raum',       CASE WHEN raum < 50000000 THEN 1 ELSE 0 END
        ) AS factors,

        -- Risk category based on score
        CASE 
            WHEN (40  * COALESCE(new_disc_flag, 0)
                + 25  * CASE WHEN raum_drop_pct >= 25 THEN 1 ELSE 0 END
                + 15  * CASE WHEN raum_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 15  * CASE WHEN client_drop_pct >= 25 THEN 1 ELSE 0 END
                +  8  * CASE WHEN client_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 10  * CASE WHEN acct_drop_pct >= 25 THEN 1 ELSE 0 END
                +  5  * CASE WHEN acct_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 10  * COALESCE(cco_changed, 0)
                + 10  * COALESCE(trend_down_flag, 0)
                + 10  * CASE WHEN owner_moves_12m >= 2 THEN 1 ELSE 0 END
                +  5  * CASE WHEN adviser_age_years < 3 THEN 1 ELSE 0 END
                +  5  * CASE WHEN raum < 50000000 THEN 1 ELSE 0 END) >= 80 THEN 'Critical'
            WHEN (40  * COALESCE(new_disc_flag, 0)
                + 25  * CASE WHEN raum_drop_pct >= 25 THEN 1 ELSE 0 END
                + 15  * CASE WHEN raum_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 15  * CASE WHEN client_drop_pct >= 25 THEN 1 ELSE 0 END
                +  8  * CASE WHEN client_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 10  * CASE WHEN acct_drop_pct >= 25 THEN 1 ELSE 0 END
                +  5  * CASE WHEN acct_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 10  * COALESCE(cco_changed, 0)
                + 10  * COALESCE(trend_down_flag, 0)
                + 10  * CASE WHEN owner_moves_12m >= 2 THEN 1 ELSE 0 END
                +  5  * CASE WHEN adviser_age_years < 3 THEN 1 ELSE 0 END
                +  5  * CASE WHEN raum < 50000000 THEN 1 ELSE 0 END) >= 60 THEN 'High'
            WHEN (40  * COALESCE(new_disc_flag, 0)
                + 25  * CASE WHEN raum_drop_pct >= 25 THEN 1 ELSE 0 END
                + 15  * CASE WHEN raum_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 15  * CASE WHEN client_drop_pct >= 25 THEN 1 ELSE 0 END
                +  8  * CASE WHEN client_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 10  * CASE WHEN acct_drop_pct >= 25 THEN 1 ELSE 0 END
                +  5  * CASE WHEN acct_drop_pct BETWEEN 15 AND 24.9 THEN 1 ELSE 0 END
                + 10  * COALESCE(cco_changed, 0)
                + 10  * COALESCE(trend_down_flag, 0)
                + 10  * CASE WHEN owner_moves_12m >= 2 THEN 1 ELSE 0 END
                +  5  * CASE WHEN adviser_age_years < 3 THEN 1 ELSE 0 END
                +  5  * CASE WHEN raum < 50000000 THEN 1 ELSE 0 END) >= 30 THEN 'Medium'
            ELSE 'Low'
        END AS risk_category

    FROM ia_change

    ON CONFLICT (crd, filing_date) 
    DO UPDATE SET 
        score = EXCLUDED.score,
        factors = EXCLUDED.factors,
        risk_category = EXCLUDED.risk_category,
        updated_at = CURRENT_TIMESTAMP;
        
    -- Log the calculation
    RAISE NOTICE 'Risk scores calculated for % records', (SELECT COUNT(*) FROM ia_risk_score WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 minute');
END;
$$;

-- Helper function to get risk statistics
CREATE OR REPLACE FUNCTION get_risk_statistics()
RETURNS TABLE (
    risk_category VARCHAR(20),
    firm_count BIGINT,
    percentage DECIMAL(5,2),
    avg_score DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        rs.risk_category,
        COUNT(*) as firm_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
        ROUND(AVG(rs.score), 2) as avg_score
    FROM ia_risk_score rs
    WHERE rs.filing_date = (SELECT MAX(filing_date) FROM ia_risk_score)
    GROUP BY rs.risk_category
    ORDER BY 
        CASE rs.risk_category 
            WHEN 'Critical' THEN 1 
            WHEN 'High' THEN 2 
            WHEN 'Medium' THEN 3 
            WHEN 'Low' THEN 4 
        END;
END;
$$ LANGUAGE plpgsql;

-- Function to get firms by risk category
CREATE OR REPLACE FUNCTION get_firms_by_risk_category(
    p_risk_category VARCHAR(20) DEFAULT NULL,
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    crd INTEGER,
    firm_name TEXT,
    score DECIMAL(5,2),
    risk_category VARCHAR(20),
    filing_date DATE,
    factors JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        rs.crd,
        COALESCE(ic.firm_name, 'Unknown') as firm_name,
        rs.score,
        rs.risk_category,
        rs.filing_date,
        rs.factors
    FROM ia_risk_score rs
    LEFT JOIN ia_change ic ON rs.crd = ic.crd AND rs.filing_date = ic.filing_date
    WHERE (p_risk_category IS NULL OR rs.risk_category = p_risk_category)
    ORDER BY rs.score DESC, rs.filing_date DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql; 