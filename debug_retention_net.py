"""
debug_retention_net.py

Day-by-day comparison of retention net deposits for March 2026:
  1. CRM  — transactions table (approved, no bonus) filtered to retention agents
  2. DDP  — dealio_daily_profits (local, fully synced) filtered to retention account logins

Run:
    docker exec reporting-system-app-1 python debug_retention_net.py
"""

from app.db.postgres_conn import get_connection

DATE_FROM = '2026-03-01'
DATE_TO   = '2026-03-31'

conn = get_connection()
with conn.cursor() as cur:

    # ── Retention agent IDs ───────────────────────────────────────────────────
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
    print(f"Retention agents: {len(retention_agent_ids):,}")

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
    print(f"Retention logins: {len(retention_logins):,}")

    # ── CRM per day ───────────────────────────────────────────────────────────
    cur.execute("""
        SELECT
            t.confirmation_time::date AS day,
            COALESCE(SUM(CASE
                WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled')  THEN  t.usdamount
                WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled')  THEN -t.usdamount
            END), 0) AS net_usd,
            COUNT(*) AS cnt
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
        GROUP BY t.confirmation_time::date
        ORDER BY day
    """, (retention_agent_ids, DATE_FROM, DATE_TO))
    crm_by_day = {str(r[0]): (float(r[1]), int(r[2])) for r in cur.fetchall()}

    # ── Dealio daily profits per day (local copy) ─────────────────────────────
    cur.execute("""
        SELECT
            date::date AS day,
            COALESCE(SUM(convertednetdeposit), 0) AS net_usd,
            COUNT(*) AS cnt
        FROM dealio_daily_profits
        WHERE login = ANY(%s)
          AND date::date >= %s
          AND date::date <= %s
        GROUP BY date::date
        ORDER BY day
    """, (retention_logins, DATE_FROM, DATE_TO))
    ddp_by_day = {str(r[0]): (float(r[1]), int(r[2])) for r in cur.fetchall()}

conn.close()

# ── Day-by-day comparison ─────────────────────────────────────────────────────
all_days = sorted(set(list(crm_by_day.keys()) + list(ddp_by_day.keys())))

print()
print(f"{'Date':<12} {'CRM':>12} {'DDP':>12} {'Diff':>12}  {'CRM txs':>8}  {'DDP rows':>8}")
print("-" * 72)

crm_total = ddp_total = 0.0
for d in all_days:
    crm_net, crm_cnt = crm_by_day.get(d, (0.0, 0))
    ddp_net, ddp_cnt = ddp_by_day.get(d, (0.0, 0))
    diff = crm_net - ddp_net
    crm_total += crm_net
    ddp_total += ddp_net
    flag = "  <<<" if abs(diff) > 5000 else ""
    print(f"{d:<12} {crm_net:>12,.0f} {ddp_net:>12,.0f} {diff:>+12,.0f}  {crm_cnt:>8,}  {ddp_cnt:>8,}{flag}")

print("-" * 72)
print(f"{'TOTAL':<12} {crm_total:>12,.0f} {ddp_total:>12,.0f} {crm_total-ddp_total:>+12,.0f}")
