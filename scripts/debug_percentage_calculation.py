#!/usr/bin/env python3
import sqlalchemy as sa
import pandas as pd

# Database connection
dsn = "postgresql+psycopg2://iapdadmin:AdvPwd#2025@127.0.0.1:5432/iapd"
engine = sa.create_engine(dsn, connect_args={"sslmode": "disable"})

try:
    with engine.connect() as conn:
        # Check client_count and account_count for any extreme values
        result = conn.execute(sa.text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN client_count IS NULL THEN 1 END) as null_clients,
                COUNT(CASE WHEN client_count < 0 THEN 1 END) as negative_clients,
                COUNT(CASE WHEN client_count = 0 THEN 1 END) as zero_clients,
                COUNT(CASE WHEN client_count > 1000000 THEN 1 END) as huge_clients,
                MAX(client_count) as max_clients,
                COUNT(CASE WHEN account_count IS NULL THEN 1 END) as null_accounts,
                COUNT(CASE WHEN account_count < 0 THEN 1 END) as negative_accounts,
                COUNT(CASE WHEN account_count = 0 THEN 1 END) as zero_accounts,
                COUNT(CASE WHEN account_count > 1000000 THEN 1 END) as huge_accounts,
                MAX(account_count) as max_accounts
            FROM ia_filing
        """))
        
        stats = result.fetchone()
        print("🔍 Client/Account Count Distribution:")
        print(f"  Total records: {stats[0]:,}")
        print(f"  NULL client_count: {stats[1]:,}")
        print(f"  Negative client_count: {stats[2]:,}")
        print(f"  Zero client_count: {stats[3]:,}")
        print(f"  Huge client_count (>1M): {stats[4]:,}")
        print(f"  Max client_count: {stats[5] if stats[5] else 'NULL'}")
        print(f"  NULL account_count: {stats[6]:,}")
        print(f"  Negative account_count: {stats[7]:,}")
        print(f"  Zero account_count: {stats[8]:,}")
        print(f"  Huge account_count (>1M): {stats[9]:,}")
        print(f"  Max account_count: {stats[10] if stats[10] else 'NULL'}")
        print()
        
        # Check for extreme percentage drops in client_count
        result = conn.execute(sa.text("""
            WITH client_changes AS (
                SELECT 
                    sec_number,
                    filing_date,
                    client_count,
                    LAG(client_count) OVER (PARTITION BY sec_number ORDER BY filing_date) as prev_client_count
                FROM ia_filing 
                ORDER BY sec_number, filing_date
            )
            SELECT 
                sec_number,
                filing_date,
                client_count,
                prev_client_count,
                CASE 
                    WHEN prev_client_count > 0 AND client_count < prev_client_count THEN
                        ((prev_client_count - client_count) / prev_client_count) * 100
                    ELSE 0
                END as drop_pct
            FROM client_changes 
            WHERE prev_client_count IS NOT NULL 
            AND prev_client_count > 0 
            AND client_count < prev_client_count
            ORDER BY drop_pct DESC
            LIMIT 10
        """))
        
        print("🔍 Top 10 Client Count Drops:")
        print("=" * 50)
        for row in result:
            sec_num, filing_date, client_count, prev_client_count, drop_pct = row
            print(f"SEC#: {sec_num}, Date: {filing_date}")
            print(f"  Clients: {client_count:,} (prev: {prev_client_count:,})")
            print(f"  Drop: {drop_pct:.2f}%")
            print()
        
        # Check for extreme percentage drops in account_count
        result = conn.execute(sa.text("""
            WITH account_changes AS (
                SELECT 
                    sec_number,
                    filing_date,
                    account_count,
                    LAG(account_count) OVER (PARTITION BY sec_number ORDER BY filing_date) as prev_account_count
                FROM ia_filing 
                ORDER BY sec_number, filing_date
            )
            SELECT 
                sec_number,
                filing_date,
                account_count,
                prev_account_count,
                CASE 
                    WHEN prev_account_count > 0 AND account_count < prev_account_count THEN
                        ((prev_account_count - account_count) / prev_account_count) * 100
                    ELSE 0
                END as drop_pct
            FROM account_changes 
            WHERE prev_account_count IS NOT NULL 
            AND prev_account_count > 0 
            AND account_count < prev_account_count
            ORDER BY drop_pct DESC
            LIMIT 10
        """))
        
        print("🔍 Top 10 Account Count Drops:")
        print("=" * 50)
        for row in result:
            sec_num, filing_date, account_count, prev_account_count, drop_pct = row
            print(f"SEC#: {sec_num}, Date: {filing_date}")
            print(f"  Accounts: {account_count:,} (prev: {prev_account_count:,})")
            print(f"  Drop: {drop_pct:.2f}%")
            print()
        
        # Let's try to run the actual populate_ia_change query to see where it fails
        print("🔍 Testing the actual populate_ia_change query...")
        try:
            result = conn.execute(sa.text("""
                INSERT INTO ia_change (
                    sec_number, filing_date, raum_drop_pct, client_drop_pct, acct_drop_pct,
                    new_disc_flag, cco_changed, trend_down_flag, owner_moves_12m, adviser_age_years, raum
                )
                SELECT 
                    curr.sec_number,
                    curr.filing_date,
                    -- Calculate percentage drops (capped at 100%)
                    CASE 
                        WHEN prev.raum > 0 AND curr.raum < prev.raum THEN
                            LEAST(((prev.raum - curr.raum) / prev.raum) * 100, 100)
                        ELSE 0
                    END as raum_drop_pct,
                    
                    CASE 
                        WHEN prev.client_count > 0 AND curr.client_count < prev.client_count THEN
                            LEAST(((prev.client_count - curr.client_count) / prev.client_count) * 100, 100)
                        ELSE 0
                    END as client_drop_pct,
                    
                    CASE 
                        WHEN prev.account_count > 0 AND curr.account_count < prev.account_count THEN
                            LEAST(((prev.account_count - curr.account_count) / prev.account_count) * 100, 100)
                        ELSE 0
                    END as acct_drop_pct,
                    
                    -- Risk flags
                    CASE 
                        WHEN curr.disciplinary_disclosures > COALESCE(prev.disciplinary_disclosures, 0) THEN TRUE
                        ELSE FALSE
                    END as new_disc_flag,
                    
                    CASE 
                        WHEN curr.cco_name != prev.cco_name THEN TRUE
                        ELSE FALSE
                    END as cco_changed,
                    
                    -- Calculate trend down flag (7% average decline over last 3 periods)
                    CASE 
                        WHEN (raum_drop_pct + 
                              COALESCE(LAG(raum_drop_pct, 1) OVER (PARTITION BY curr.sec_number ORDER BY curr.filing_date), 0) +
                              COALESCE(LAG(raum_drop_pct, 2) OVER (PARTITION BY curr.sec_number ORDER BY curr.filing_date), 0)) / 3 >= 7
                        THEN TRUE
                        ELSE FALSE
                    END as trend_down_flag,
                    
                    -- Additional risk factors (placeholder for now)
                    0 as owner_moves_12m,
                    EXTRACT(YEAR FROM curr.filing_date) - EXTRACT(YEAR FROM MIN(curr.filing_date) OVER (PARTITION BY curr.sec_number)) as adviser_age_years,
                    curr.raum
                FROM ia_filing curr
                LEFT JOIN ia_filing prev ON curr.sec_number = prev.sec_number 
                    AND prev.filing_date = (
                        SELECT MAX(filing_date) 
                        FROM ia_filing 
                        WHERE sec_number = curr.sec_number 
                        AND filing_date < curr.filing_date
                    )
                WHERE prev.sec_number IS NOT NULL
                LIMIT 1
            """))
            print("✅ Test insert worked!")
        except Exception as e:
            print(f"❌ Test insert failed: {e}")

except Exception as e:
    print(f"❌ Error: {e}") 