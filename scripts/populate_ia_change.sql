-- Populate ia_change table for risk scoring
-- This script calculates change metrics between consecutive filings for each firm

-- Clear existing data
TRUNCATE TABLE ia_change;

-- Insert change data by comparing consecutive filings
INSERT INTO ia_change (
    sec_number,
    filing_date,
    raum_drop_pct,
    client_drop_pct,
    acct_drop_pct,
    new_disc_flag,
    cco_changed,
    trend_down_flag,
    owner_moves_12m,
    adviser_age_years,
    raum
)
WITH filing_changes AS (
    SELECT 
        f1.sec_number,
        f1.filing_date,
        f1.raum,
        f1.client_count,
        f1.account_count,
        f1.disciplinary_disclosures,
        f1.cco_name,
        f2.raum as prev_raum,
        f2.client_count as prev_client_count,
        f2.account_count as prev_account_count,
        f2.disciplinary_disclosures as prev_disciplinary_disclosures,
        f2.cco_name as prev_cco_name,
        f2.filing_date as prev_filing_date,
        -- Calculate percentage changes (capped at 100%)
        CASE 
            WHEN f2.raum > 0 AND f1.raum < f2.raum THEN
                CASE 
                    WHEN ((f2.raum - f1.raum) / f2.raum) * 100 > 100 THEN 100
                    ELSE ((f2.raum - f1.raum) / f2.raum) * 100
                END
            ELSE 0
        END as raum_drop_pct,
        CASE 
            WHEN f2.client_count > 0 AND f1.client_count < f2.client_count THEN
                CASE 
                    WHEN ((f2.client_count - f1.client_count) / f2.client_count) * 100 > 100 THEN 100
                    ELSE ((f2.client_count - f1.client_count) / f2.client_count) * 100
                END
            ELSE 0
        END as client_drop_pct,
        CASE 
            WHEN f2.account_count IS NOT NULL AND f2.account_count > 0 AND f1.account_count IS NOT NULL THEN
                CASE 
                    WHEN ((f2.account_count - f1.account_count) / f2.account_count) * 100 > 100 THEN 100
                    ELSE ((f2.account_count - f1.account_count) / f2.account_count) * 100
                END
            ELSE 0
        END as acct_drop_pct,
        -- Check for new disciplinary disclosures
        CASE 
            WHEN f1.disciplinary_disclosures > f2.disciplinary_disclosures THEN TRUE
            ELSE FALSE 
        END as new_disc_flag,
        -- Check for CCO changes
        CASE 
            WHEN f1.cco_name != f2.cco_name AND f1.cco_name IS NOT NULL AND f2.cco_name IS NOT NULL THEN TRUE
            ELSE FALSE 
        END as cco_changed,
        -- Calculate adviser age (years since first filing)
        EXTRACT(YEAR FROM f1.filing_date) - EXTRACT(YEAR FROM MIN(f1.filing_date) OVER (PARTITION BY f1.sec_number)) as adviser_age_years
    FROM ia_filing f1
    LEFT JOIN LATERAL (
        SELECT 
            sec_number,
            filing_date,
            raum,
            client_count,
            account_count,
            disciplinary_disclosures,
            cco_name
        FROM ia_filing f2
        WHERE f2.sec_number = f1.sec_number 
        AND f2.filing_date < f1.filing_date
        ORDER BY f2.filing_date DESC
        LIMIT 1
    ) f2 ON true
    WHERE f1.raum > 0  -- Only include firms with RAUM data
),
trend_analysis AS (
    SELECT 
        sec_number,
        filing_date,
        raum_drop_pct,
        client_drop_pct,
        acct_drop_pct,
        new_disc_flag,
        cco_changed,
        adviser_age_years,
        raum,
        -- Calculate trend down flag (7% average decline over last 3 periods)
        CASE 
            WHEN (raum_drop_pct + 
                  COALESCE(LAG(raum_drop_pct, 1) OVER (PARTITION BY sec_number ORDER BY filing_date), 0) +
                  COALESCE(LAG(raum_drop_pct, 2) OVER (PARTITION BY sec_number ORDER BY filing_date), 0)) / 3 >= 7
            THEN TRUE
            ELSE FALSE
        END as trend_down_flag,
        -- For now, set owner_moves_12m to 0 (would need additional data)
        0 as owner_moves_12m
    FROM filing_changes
)
SELECT 
    sec_number,
    filing_date,
    raum_drop_pct,
    client_drop_pct,
    acct_drop_pct,
    new_disc_flag,
    cco_changed,
    trend_down_flag,
    owner_moves_12m,
    adviser_age_years,
    raum
FROM trend_analysis
WHERE filing_date IS NOT NULL;

-- Log the results
DO $$
BEGIN
    RAISE NOTICE 'Populated ia_change table with % records', (SELECT COUNT(*) FROM ia_change);
    RAISE NOTICE 'Firms with new disciplinary disclosures: %', (SELECT COUNT(*) FROM ia_change WHERE new_disc_flag = TRUE);
    RAISE NOTICE 'Firms with CCO changes: %', (SELECT COUNT(*) FROM ia_change WHERE cco_changed = TRUE);
    RAISE NOTICE 'Firms with AUM drops >= 25%%: %', (SELECT COUNT(*) FROM ia_change WHERE raum_drop_pct >= 25);
END $$; 