import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from datetime import date, timedelta

DEALIO = {
    '2026-03-23': -155844.47,
    '2026-03-22': -14424.70,
    '2026-03-21': -44154.84,
    '2026-03-20': -270655.16,
    '2026-03-19': -422664.80,
    '2026-03-18': -654360.68,
    '2026-03-17': -175503.58,
    '2026-03-16': -191888.22,
    '2026-03-15': -1527.01,
    '2026-03-14': -57922.57,
    '2026-03-13': -306280.28,
    '2026-03-12': -389207.58,
    '2026-03-11': -191393.41,
    '2026-03-10': -94138.67,
    '2026-03-09': -606834.62,
    '2026-03-08': -9572.73,
    '2026-03-07': -56967.27,
    '2026-03-06': 361107.87,
    '2026-03-05': -279956.67,
    '2026-03-04': 370.40,
    '2026-03-03': -708348.23,
    '2026-03-02': -301158.91,
    '2026-03-01': -10417.25,
}

conn = get_connection()
try:
    with conn.cursor() as cur:

        # Valid (non-test) logins — same filter as live formula
        cur.execute("""
            SELECT ta.login::bigint
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
              AND a.is_test_account = 0
              AND ta.vtigeraccountid IS NOT NULL
        """)
        valid_logins = [r[0] for r in cur.fetchall()]
        print(f"Valid logins: {len(valid_logins)}")

        # Our formula: MAX(0, convertedbalance + convertedfloatingpnl) per login per day
        # Matches start_net_equity / daily_end_net_equity logic in live_equity.py
        cur.execute("""
            SELECT DISTINCT ON (date::date, login)
                date::date AS day,
                login,
                COALESCE(convertedbalance, 0) + COALESCE(convertedfloatingpnl, 0) AS net_equity
            FROM dealio_daily_profits
            WHERE date >= '2026-02-28' AND date < '2026-03-24'
              AND login = ANY(%s)
            ORDER BY date::date, login, date DESC
        """, (valid_logins,))
        equity = {}
        for r in cur.fetchall():
            equity[(int(r[1]), str(r[0]))] = float(r[2]) if r[2] else 0.0

        # Net deposits — same filter as live formula
        cur.execute("""
            SELECT t.confirmation_time::date AS day,
                   SUM(CASE
                       WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                       WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                   END) AS net_dep
            FROM transactions t
            JOIN crm_users u ON u.id = t.original_deposit_owner
            JOIN accounts a  ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND t.confirmation_time >= '2026-03-01'
              AND t.confirmation_time <  '2026-03-24'
              AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
              AND t.vtigeraccountid IS NOT NULL
              AND a.is_test_account = 0
              AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%bonus%'
            GROUP BY t.confirmation_time::date
            ORDER BY day
        """)
        dep_rows = {str(r[0]): float(r[1]) for r in cur.fetchall()}

        # Bonuses per day — same as live formula
        cur.execute("""
            SELECT confirmation_time::date AS day, COALESCE(SUM(net_amount), 0) AS bonuses
            FROM bonus_transactions
            WHERE confirmation_time >= '2026-03-01'
              AND confirmation_time <  '2026-03-24'
            GROUP BY confirmation_time::date
            ORDER BY day
        """)
        bonus_rows = {str(r[0]): float(r[1]) for r in cur.fetchall()}

finally:
    conn.close()

all_logins = set(k[0] for k in equity.keys())
dates = sorted(DEALIO.keys())

def fmt(v): return f"${v:,.2f}" if v is not None else "N/A"

print(f"\n{'Date':<12} {'Dealio Ref':>14} {'Our Formula':>14} {'Diff':>12}")
print("-" * 56)

dealio_total = 0.0
ours_total   = 0.0

for d in dates:
    dealio_v = DEALIO[d]
    prev     = str(date.fromisoformat(d) - timedelta(days=1))
    net_dep  = dep_rows.get(d, 0.0)
    bonuses  = bonus_rows.get(d, 0.0)

    end_eq   = sum(max(0.0, equity.get((l, d),    0.0)) for l in all_logins if (l, d)    in equity)
    start_eq = sum(max(0.0, equity.get((l, prev), 0.0)) for l in all_logins if (l, prev) in equity)
    ours = round(end_eq - start_eq - net_dep - bonuses, 2) if (end_eq or start_eq) else None

    diff = round(ours - dealio_v, 2) if ours is not None else None

    if ours is not None: ours_total   += ours
    dealio_total += dealio_v

    print(f"{d:<12} {fmt(dealio_v):>14} {fmt(ours):>14} {fmt(diff):>12}")

print("-" * 56)
print(f"{'TOTAL':<12} {fmt(dealio_total):>14} {fmt(ours_total):>14} {fmt(ours_total-dealio_total):>12}")
