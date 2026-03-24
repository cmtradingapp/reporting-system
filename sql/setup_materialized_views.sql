-- =============================================================================
-- Materialized Views for Reporting System
-- Pages covered: Performance Report, Agent Bonuses, Dashboard, FTC Date (partial)
--
-- Run once on the server:
--   psql -U <user> -d <db> -f setup_materialized_views.sql
--
-- Subsequent refreshes are handled automatically every 2 min by APScheduler
-- (refresh_materialized_views in postgres_conn.py) and optionally by pg_cron.
-- =============================================================================


-- =============================================================================
-- DROP ORDER: run_rate depends on daily_kpis — drop it first
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS mv_run_rate      CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_bonuses       CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_volume_stats  CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_kpis    CASCADE;


-- =============================================================================
-- 1.  mv_daily_kpis
-- =============================================================================
-- Grain   : (agent_id, tx_date, qual_date)
-- Sources : transactions + accounts + crm_users
-- Serves  : Performance Report, Agent Bonuses, Dashboard, FTC Date
--
-- Both date axes are kept so queries can filter on either:
--   - tx_date  (confirmation_time)  → NET, FTD, deposits, withdrawals
--   - qual_date (client_qualification_date) → FTC
--
-- Filters already baked in:
--   • Approved transactions only
--   • Non-deleted
--   • Non-test accounts (is_test_account = 0)
--   • Non-test agents   (agent_name NOT ILIKE 'test%')
--   • No bonus transactions (comment NOT LIKE '%bonus%')
--   • vtigeraccountid IS NOT NULL
-- =============================================================================
CREATE MATERIALIZED VIEW mv_daily_kpis AS
SELECT
    t.original_deposit_owner                                             AS agent_id,
    t.confirmation_time::date                                            AS tx_date,
    a.client_qualification_date::date                                    AS qual_date,
    -- NET deposits (positive = net in, negative = net out)
    COALESCE(SUM(CASE
        WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled')  THEN  t.usdamount
        WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled')  THEN -t.usdamount
    END), 0)                                                             AS net_usd,
    -- Gross deposits only
    COALESCE(SUM(CASE
        WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled')  THEN t.usdamount
        ELSE 0
    END), 0)                                                             AS deposit_usd,
    -- Gross withdrawals only
    COALESCE(SUM(CASE
        WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled')  THEN t.usdamount
        ELSE 0
    END), 0)                                                             AS withdrawal_usd,
    -- FTD count by tx_date (for Grand FTD, Daily FTD, per-agent FTD)
    SUM(CASE WHEN t.transactiontype = 'Deposit' AND t.ftd = 1 THEN 1 ELSE 0 END)::int
                                                                         AS ftd_count,
    -- FTC count by qual_date (for Grand FTC, Daily FTC, per-agent FTC)
    COUNT(DISTINCT CASE WHEN t.transactiontype = 'Deposit' AND t.ftd = 1
                        THEN t.vtigeraccountid END)::int                 AS ftc_count
FROM transactions t
JOIN accounts  a ON a.accountid           = t.vtigeraccountid
JOIN crm_users u ON u.id                  = t.original_deposit_owner
WHERE t.transactionapproval = 'Approved'
  AND (t.deleted = 0 OR t.deleted IS NULL)
  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
  AND t.vtigeraccountid IS NOT NULL
  AND a.is_test_account = 0
  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%bonus%'
GROUP BY
    t.original_deposit_owner,
    t.confirmation_time::date,
    a.client_qualification_date::date;

-- Unique index required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX idx_mv_daily_kpis_u
    ON mv_daily_kpis (agent_id, tx_date, COALESCE(qual_date, '1900-01-01'::date));
CREATE INDEX idx_mv_daily_kpis_tx_date   ON mv_daily_kpis (tx_date);
CREATE INDEX idx_mv_daily_kpis_qual_date ON mv_daily_kpis (qual_date) WHERE qual_date IS NOT NULL;
CREATE INDEX idx_mv_daily_kpis_agent     ON mv_daily_kpis (agent_id);


-- =============================================================================
-- 2.  mv_volume_stats
-- =============================================================================
-- Grain   : (agent_id, accountid, open_date)
-- Sources : dealio_trades_mt4 + trading_accounts + accounts + crm_users
-- Serves  : Performance Report (Open Volume), Agent Bonuses (Volume),
--           Dashboard (Open Volume Q7, Traders Q6)
--
-- NOTE: ABS Exposure (Dashboard Q9) is NOT covered here because it requires
--       filtering on close_time = '1970-01-01' (currently-open positions only),
--       which is a snapshot not a date-range aggregate.  Q9 stays live.
--
-- Filters baked in:
--   • vtigeraccountid IS NOT NULL
--   • Non-test accounts
--   • Non-test agents (via LEFT JOIN crm_users)
--   • open_date >= 2024-01-01
-- =============================================================================
CREATE MATERIALIZED VIEW mv_volume_stats AS
SELECT
    a.assigned_to                                                        AS agent_id,
    ta.vtigeraccountid                                                   AS accountid,
    d.open_time::date                                                    AS open_date,
    SUM(d.notional_value)                                                AS notional_usd,
    -- 1 if any trade on this date had notional_value > 0 (used for Traders count)
    MAX(CASE WHEN d.notional_value > 0 THEN 1 ELSE 0 END)::smallint     AS has_positive_notional
FROM dealio_trades_mt4 d
JOIN trading_accounts ta ON ta.login::bigint        = d.login
JOIN accounts         a  ON a.accountid             = ta.vtigeraccountid
LEFT JOIN crm_users   u  ON u.id                    = a.assigned_to
WHERE ta.vtigeraccountid IS NOT NULL
  AND a.is_test_account = 0
  AND d.open_time::date >= '2024-01-01'
  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
GROUP BY
    a.assigned_to,
    ta.vtigeraccountid,
    d.open_time::date;

CREATE UNIQUE INDEX idx_mv_volume_stats_u
    ON mv_volume_stats (COALESCE(agent_id, -1), accountid, open_date);
CREATE INDEX idx_mv_volume_stats_open_date ON mv_volume_stats (open_date);
CREATE INDEX idx_mv_volume_stats_agent     ON mv_volume_stats (agent_id);
CREATE INDEX idx_mv_volume_stats_account   ON mv_volume_stats (accountid);


-- =============================================================================
-- 3.  mv_bonuses
-- =============================================================================
-- Grain   : (agent_id, ftd_100_date)
-- Source  : ftd100_clients
-- Serves  : Agent Bonuses – Sales section
--           (FTD100 count, FTD amount bonus tier, net_until_qualification)
--
-- The tiered FTD amount bonus is pre-computed in SQL:
--   < $500  → $0   |  $500–$999  → $10
--   $1000–$4999 → $20  |  >= $5000 → $50
-- =============================================================================
CREATE MATERIALIZED VIEW mv_bonuses AS
SELECT
    f.original_deposit_owner                                             AS agent_id,
    f.ftd_100_date,
    COUNT(DISTINCT f.accountid)::int                                     AS ftd100_count,
    COALESCE(SUM(f.net_until_qualification), 0)                         AS total_sales_net,
    COALESCE(SUM(CASE
        WHEN f.ftd_100_amount < 500   THEN 0
        WHEN f.ftd_100_amount < 1000  THEN 10
        WHEN f.ftd_100_amount < 5000  THEN 20
        ELSE 50
    END), 0)::float                                                      AS ftd_amount_bonus
FROM ftd100_clients f
WHERE f.original_deposit_owner IS NOT NULL
GROUP BY
    f.original_deposit_owner,
    f.ftd_100_date;

CREATE UNIQUE INDEX idx_mv_bonuses_u      ON mv_bonuses (agent_id, ftd_100_date);
CREATE INDEX idx_mv_bonuses_date          ON mv_bonuses (ftd_100_date);
CREATE INDEX idx_mv_bonuses_agent         ON mv_bonuses (agent_id);


-- =============================================================================
-- 4.  mv_run_rate
-- =============================================================================
-- Grain   : (dept_group, tx_date, qual_date)
-- Source  : mv_daily_kpis + crm_users  (derived MV — refresh AFTER mv_daily_kpis)
-- Serves  : Dashboard (Q1–Q5 NET/FTD/FTC daily+monthly),
--           Performance Report (Grand NET, Grand FTC, Grand FTD, Daily totals)
--
-- dept_group values:
--   'all'       – every agent in mv_daily_kpis (test-agent filter already applied)
--   'sales'     – department_='Sales', team='Conversion', not test/duplicated
--   'retention' – department_='Retention', not test/duplicated/manager
--   'other'     – everything else
--
-- This is a tiny table (≤ 4 × 365 × ~2 years × qual_date variants).
-- Dashboard queries become single indexed range scans instead of full table
-- scans of the transactions table.
-- =============================================================================
CREATE MATERIALIZED VIEW mv_run_rate AS

-- dept-tagged rows (sales / retention / other)
WITH tagged AS (
    SELECT
        k.agent_id,
        k.tx_date,
        k.qual_date,
        k.net_usd,
        k.deposit_usd,
        k.ftd_count,
        k.ftc_count,
        CASE
            WHEN u.department_ = 'Sales'
             AND u.team = 'Conversion'
             AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%'
             AND TRIM(COALESCE(u.full_name, ''))               NOT ILIKE 'test%'
            THEN 'sales'
            WHEN u.department_ = 'Retention'
             AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%'
             AND TRIM(COALESCE(u.full_name, ''))               NOT ILIKE 'test%'
             AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%Retention%'
             AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%Conversion%'
             AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%Support%'
             AND TRIM(COALESCE(u.department, ''))              NOT ILIKE '%General%'
            THEN 'retention'
            ELSE 'other'
        END AS dept_group
    FROM mv_daily_kpis k
    JOIN crm_users u ON u.id = k.agent_id
)
SELECT
    dept_group,
    tx_date,
    qual_date,
    SUM(net_usd)     AS net_usd,
    SUM(deposit_usd) AS deposit_usd,
    SUM(ftd_count)   AS ftd_count,
    SUM(ftc_count)   AS ftc_count
FROM tagged
GROUP BY dept_group, tx_date, qual_date

UNION ALL

-- 'all' group — every non-test agent already in mv_daily_kpis
SELECT
    'all'            AS dept_group,
    tx_date,
    qual_date,
    SUM(net_usd)     AS net_usd,
    SUM(deposit_usd) AS deposit_usd,
    SUM(ftd_count)   AS ftd_count,
    SUM(ftc_count)   AS ftc_count
FROM mv_daily_kpis
GROUP BY tx_date, qual_date;

CREATE UNIQUE INDEX idx_mv_run_rate_u
    ON mv_run_rate (dept_group, tx_date, COALESCE(qual_date, '1900-01-01'::date));
CREATE INDEX idx_mv_run_rate_dept     ON mv_run_rate (dept_group);
CREATE INDEX idx_mv_run_rate_tx_date  ON mv_run_rate (tx_date);
CREATE INDEX idx_mv_run_rate_qual     ON mv_run_rate (qual_date) WHERE qual_date IS NOT NULL;


-- =============================================================================
-- 5.  pg_cron refresh schedule  (requires pg_cron extension)
--     APScheduler in main.py is the primary refresh mechanism.
--     Uncomment below only if pg_cron is installed as a backup.
-- =============================================================================
-- SELECT cron.unschedule('refresh-mv-reporting');
-- SELECT cron.schedule(
--     'refresh-mv-reporting',
--     '* * * * *',   -- every minute
--     $$
--         REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_kpis;
--         REFRESH MATERIALIZED VIEW CONCURRENTLY mv_volume_stats;
--         REFRESH MATERIALIZED VIEW CONCURRENTLY mv_bonuses;
--         REFRESH MATERIALIZED VIEW CONCURRENTLY mv_run_rate;
--     $$
-- );


-- =============================================================================
-- FTC Date page note
-- =============================================================================
-- The /api/ftc-date endpoint uses highly dynamic filters (end_date, agent_id,
-- office, team, classification) and computes `days_diff = end_date - qual_date`
-- at query time, which changes with every request.  The inner CTEs (rdp,
-- withdrawalers, traders) also have date cutoffs that depend on end_date.
--
-- Practical optimisation for ftc_date without MVs:
--   1. Add an index on accounts(client_qualification_date) if missing.
--   2. Add an index on transactions(vtigeraccountid, transactiontype) if missing.
--
-- These DDL statements are safe to run now:
CREATE INDEX IF NOT EXISTS idx_accounts_qual_date
    ON accounts (client_qualification_date);
CREATE INDEX IF NOT EXISTS idx_transactions_account_type
    ON transactions (vtigeraccountid, transactiontype)
    WHERE transactionapproval = 'Approved';
