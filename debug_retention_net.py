"""
debug_retention_net.py

Compares retention net deposits for March 2026:
  1. Our system   — transactions table (CRM, approved) filtered to retention agents
  2. Dealio MT4   — dealio_trades_mt4 cmd=6 filtered to logins of retention accounts

Run:
    docker exec reporting-system-app-1 python debug_retention_net.py
"""

from app.db.postgres_conn import get_connection

DATE_FROM = '2026-03-01'
DATE_TO   = '2026-03-31'

conn = get_connection()
with conn.cursor() as cur:

    # ── Retention agent IDs (same filter as scoreboard retention table) ───────
    cur.execute("""
        SELECT id FROM crm_users
        WHERE department_ = 'Retention'
          AND TRIM(COALESCE(agent_name, full_name, '')) NOT ILIKE 'test%'
          AND TRIM(COALESCE(full_name, ''))             NOT ILIKE 'test%'
          AND TRIM(COALESCE(agent_name, full_name, '')) NOT ILIKE 'duplicated%'
          AND TRIM(COALESCE(department, ''))            NOT ILIKE '%Retention%'
          AND TRIM(COALESCE(department, ''))            NOT ILIKE '%Conversion%'
          AND TRIM(COALESCE(department, ''))            NOT ILIKE '%Support%'
          AND TRIM(COALESCE(department, ''))            NOT ILIKE '%General%'
    """)
    retention_agent_ids = [r[0] for r in cur.fetchall()]
    print(f"Retention agents: {len(retention_agent_ids)}")

    # ── SOURCE 1: Our transactions table (CRM) ────────────────────────────────
    cur.execute("""
        SELECT
            COALESCE(SUM(CASE
                WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled')  THEN  t.usdamount
                WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled')  THEN -t.usdamount
            END), 0) AS net_usd,
            COUNT(*) AS tx_count
        FROM transactions t
        JOIN accounts a ON a.accountid = t.vtigeraccountid
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
          AND t.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0
          AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
          AND t.original_deposit_owner = ANY(%s)
          AND t.confirmation_time::date >= %s
          AND t.confirmation_time::date <= %s
    """, (retention_agent_ids, DATE_FROM, DATE_TO))
    row = cur.fetchone()
    crm_net, crm_count = float(row[0] or 0), int(row[1] or 0)

    # ── SOURCE 2: dealio_trades_mt4 cmd=6 for retention account logins ────────
    # Get logins belonging to retention-assigned accounts
    cur.execute("""
        SELECT DISTINCT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE a.assigned_to = ANY(%s)
          AND a.is_test_account = 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
    """, (retention_agent_ids,))
    retention_logins = [r[0] for r in cur.fetchall()]
    print(f"Retention account logins: {len(retention_logins):,}")

    cur.execute("""
        SELECT
            COALESCE(SUM(COALESCE(computed_profit, 0)), 0) AS net_usd,
            COUNT(*) AS tx_count
        FROM dealio_trades_mt4
        WHERE cmd = 6
          AND login = ANY(%s)
          AND close_time::date >= %s
          AND close_time::date <= %s
    """, (retention_logins, DATE_FROM, DATE_TO))
    row = cur.fetchone()
    mt4_net, mt4_count = float(row[0] or 0), int(row[1] or 0)

    # ── SOURCE 2b: cmd=6 excluding adjustment symbols ─────────────────────────
    cur.execute("""
        SELECT
            COALESCE(SUM(COALESCE(computed_profit, 0)), 0) AS net_usd,
            COUNT(*) AS tx_count
        FROM dealio_trades_mt4
        WHERE cmd = 6
          AND login = ANY(%s)
          AND close_time::date >= %s
          AND close_time::date <= %s
          AND (symbol IS NULL OR symbol = ''
               OR symbol NOT IN (
                   'Cashback','CFDRollover','CommEUR','CommUSD','CorrectiEUR','CorrectiGBP',
                   'CorrectiJPY','Correction','CredExp','CredExpEUR','CredExpGBP','CredExpJPY',
                   'Dividend','DividendEUR','DividendGBP','DividendJPY','Dormant','EarnedCr',
                   'EarnedCrEUR','FEE','INACT-FEE','Inactivity','Rollover','SPREAD',
                   'ZeroingEUR','ZeroingGBP','ZeroingJPY','ZeroingKES','ZeroingNGN',
                   'ZeroingUSD','ZeroingZAR'
               ))
    """, (retention_logins, DATE_FROM, DATE_TO))
    row = cur.fetchone()
    mt4_cash_net, mt4_cash_count = float(row[0] or 0), int(row[1] or 0)

    # ── Breakdown of cmd=6 by symbol ─────────────────────────────────────────
    print()
    print("cmd=6 breakdown by symbol (retention logins, March 2026):")
    cur.execute("""
        SELECT COALESCE(symbol, '(no symbol)') AS sym,
               COUNT(*) AS cnt,
               COALESCE(SUM(computed_profit), 0) AS total
        FROM dealio_trades_mt4
        WHERE cmd = 6
          AND login = ANY(%s)
          AND close_time::date >= %s
          AND close_time::date <= %s
        GROUP BY symbol
        ORDER BY ABS(SUM(computed_profit)) DESC
        LIMIT 20
    """, (retention_logins, DATE_FROM, DATE_TO))
    for r in cur.fetchall():
        print(f"  {r[0]:<25} {r[1]:>6,} ops   {float(r[2]):>12,.0f}")

conn.close()

print()
print("=" * 60)
print(f"Date range:              {DATE_FROM} – {DATE_TO}")
print(f"Retention agents:        {len(retention_agent_ids):,}")
print(f"Retention logins:        {len(retention_logins):,}")
print()
print(f"CRM transactions:        ${crm_net:>12,.0f}  ({crm_count:,} txs)")
print(f"MT4 cmd=6 all symbols:   ${mt4_net:>12,.0f}  ({mt4_count:,} ops)")
print(f"MT4 cmd=6 cash only:     ${mt4_cash_net:>12,.0f}  ({mt4_cash_count:,} ops)")
print()
print(f"Dealio known:            ${3_055_666:>12,}")
print(f"Our scoreboard:          ${3_145_294:>12,}")
print()
print(f"MT4 all   vs Dealio:     {mt4_net - 3_055_666:>+12,.0f}")
print(f"MT4 cash  vs Dealio:     {mt4_cash_net - 3_055_666:>+12,.0f}")
print(f"CRM       vs Dealio:     {crm_net - 3_055_666:>+12,.0f}")
