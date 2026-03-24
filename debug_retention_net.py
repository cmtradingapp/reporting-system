"""
debug_retention_net.py

Compares retention net deposits for March 2026:
  1. Our system   — transactions table (CRM, approved) filtered to retention agents
  2. Dealio MT4   — dealio.trades_mt4 cmd=6 (replica) filtered to retention account logins

Run:
    docker exec reporting-system-app-1 python debug_retention_net.py
"""

from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection

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

    # ── Retention account logins ──────────────────────────────────────────────
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

conn.close()

# ── SOURCE 2: Dealio replica — dealio.trades_mt4 cmd=6 ───────────────────────
# Chunk logins to avoid SSL timeout on large arrays
CHUNK = 5000
_ADJ = ('Cashback','CFDRollover','CommEUR','CommUSD','CorrectiEUR','CorrectiGBP',
        'CorrectiJPY','Correction','CredExp','CredExpEUR','CredExpGBP','CredExpJPY',
        'Dividend','DividendEUR','DividendGBP','DividendJPY','Dormant','EarnedCr',
        'EarnedCrEUR','FEE','INACT-FEE','Inactivity','Rollover','SPREAD',
        'ZeroingEUR','ZeroingGBP','ZeroingJPY','ZeroingKES','ZeroingNGN',
        'ZeroingUSD','ZeroingZAR')

mt4_net       = 0.0;  mt4_count      = 0
mt4_cash_net  = 0.0;  mt4_cash_count = 0
symbol_totals = {}

chunks = [retention_logins[i:i+CHUNK] for i in range(0, len(retention_logins), CHUNK)]
print(f"Querying Dealio replica in {len(chunks)} chunks...")

for i, chunk in enumerate(chunks, 1):
    dc = get_dealio_connection()
    try:
        with dc.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(COALESCE(computed_profit, profit, 0)), 0),
                    COUNT(*)
                FROM dealio.trades_mt4
                WHERE cmd = 6
                  AND login = ANY(%s)
                  AND close_time::date >= %s
                  AND close_time::date <= %s
            """, (chunk, DATE_FROM, DATE_TO))
            row = cur.fetchone()
            mt4_net   += float(row[0] or 0)
            mt4_count += int(row[1] or 0)

            cur.execute("""
                SELECT
                    COALESCE(SUM(COALESCE(computed_profit, profit, 0)), 0),
                    COUNT(*)
                FROM dealio.trades_mt4
                WHERE cmd = 6
                  AND login = ANY(%s)
                  AND close_time::date >= %s
                  AND close_time::date <= %s
                  AND (symbol IS NULL OR symbol = ''
                       OR symbol NOT IN %s)
            """, (chunk, DATE_FROM, DATE_TO, _ADJ))
            row = cur.fetchone()
            mt4_cash_net   += float(row[0] or 0)
            mt4_cash_count += int(row[1] or 0)

            cur.execute("""
                SELECT COALESCE(symbol, '') AS sym,
                       COUNT(*) AS cnt,
                       COALESCE(SUM(COALESCE(computed_profit, profit)), 0) AS total
                FROM dealio.trades_mt4
                WHERE cmd = 6
                  AND login = ANY(%s)
                  AND close_time::date >= %s
                  AND close_time::date <= %s
                GROUP BY symbol
            """, (chunk, DATE_FROM, DATE_TO))
            for r in cur.fetchall():
                sym = str(r[0]) or '(no symbol)'
                symbol_totals[sym] = symbol_totals.get(sym, (0, 0.0))
                symbol_totals[sym] = (symbol_totals[sym][0] + int(r[1]),
                                      symbol_totals[sym][1] + float(r[2]))
    finally:
        dc.close()
    print(f"  chunk {i}/{len(chunks)} done")

print()
print("cmd=6 breakdown by symbol (retention logins, March 2026):")
for sym, (cnt, total) in sorted(symbol_totals.items(), key=lambda x: -abs(x[1][1]))[:20]:
    print(f"  {sym:<25} {cnt:>6,} ops   {total:>12,.0f}")

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
print(f"MT4 all   vs Dealio:     {mt4_net   - 3_055_666:>+12,.0f}")
print(f"MT4 cash  vs Dealio:     {mt4_cash_net - 3_055_666:>+12,.0f}")
print(f"CRM       vs Dealio:     {crm_net   - 3_055_666:>+12,.0f}")
