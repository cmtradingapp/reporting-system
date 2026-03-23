import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
from datetime import date, timedelta
from collections import defaultdict

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

pg_conn = get_connection()
dealio_conn = get_dealio_connection()

try:
    with pg_conn.cursor() as cur:
        # Step 1: Get valid (non-test) logins
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

        # Step 2: Floating PnL per login per day from local dealio_daily_profits
        cur.execute("""
            SELECT DISTINCT ON (date::date, login)
                date::date AS day,
                login,
                convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date >= '2026-02-28' AND date < '2026-03-24'
              AND login = ANY(%s)
            ORDER BY date::date, login, date DESC
        """, (valid_logins,))
        floating = {}
        for r in cur.fetchall():
            floating[(int(r[1]), str(r[0]))] = float(r[2]) if r[2] else 0.0

        # Step 3: Net deposits per day
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

    # Step 4: Cumulative credit per login per day from Dealio source cmd=7
    # No login filter here — filter in Python to avoid passing 365K logins over SSL
    valid_logins_set = set(valid_logins)
    with dealio_conn.cursor() as cur:
        cur.execute("""
            SELECT login,
                   close_time::date AS day,
                   SUM(COALESCE(computed_profit, profit)) AS credit_change
            FROM dealio.trades_mt4
            WHERE cmd = 7
              AND close_time < '2026-03-24'
            GROUP BY login, close_time::date
            ORDER BY login, day
        """)
        credit_rows = [(r[0], r[1], r[2]) for r in cur.fetchall() if int(r[0]) in valid_logins_set]
        print(f"Credit (cmd=7) rows fetched: {len(credit_rows)}")

finally:
    pg_conn.close()
    dealio_conn.close()

# Build credit changes dict: {login: sorted list of (day_str, change)}
credit_changes = defaultdict(list)
for row in credit_rows:
    credit_changes[int(row[0])].append((str(row[1]), float(row[2]) if row[2] else 0.0))

def get_credit_as_of(login, target_day_str):
    """Cumulative cmd=7 credit for login up to and including target_day."""
    total = 0.0
    for day_str, change in credit_changes.get(login, []):
        if day_str <= target_day_str:
            total += change
    return total

# All logins that appear in floating data
all_logins = set(k[0] for k in floating.keys())

dates = sorted(DEALIO.keys())

print(f"\n{'Date':<12} {'Dealio Ref':>14} {'CEO (no cred)':>15} {'With Credit':>14} {'Diff CEO':>12} {'Diff Credit':>14}")
print("-" * 90)

dealio_total   = 0.0
ceo_total      = 0.0
credit_total   = 0.0

for d in dates:
    dealio_v = DEALIO[d]
    prev     = str(date.fromisoformat(d) - timedelta(days=1))
    net_dep  = dep_rows.get(d, 0.0)

    # CEO formula: MAX(0, floatingpnl) — no credit deduction
    end_ceo   = sum(max(0.0, floating.get((l, d),    0.0)) for l in all_logins if (l, d)    in floating)
    start_ceo = sum(max(0.0, floating.get((l, prev), 0.0)) for l in all_logins if (l, prev) in floating)
    ceo = round(end_ceo - start_ceo - net_dep, 2) if (end_ceo or start_ceo) else None

    # Credit formula: MAX(0, floatingpnl - credit)
    end_cr, start_cr = 0.0, 0.0
    for l in all_logins:
        if (l, d) in floating:
            end_cr   += max(0.0, floating[(l, d)]    - get_credit_as_of(l, d))
        if (l, prev) in floating:
            start_cr += max(0.0, floating[(l, prev)] - get_credit_as_of(l, prev))
    formula = round(end_cr - start_cr - net_dep, 2)

    diff_ceo     = round(ceo     - dealio_v, 2) if ceo     is not None else None
    diff_formula = round(formula - dealio_v, 2)

    if ceo     is not None: ceo_total     += ceo
    if formula is not None: credit_total  += formula
    dealio_total += dealio_v

    def fmt(v): return f"${v:,.2f}" if v is not None else "N/A"

    print(f"{d:<12} {fmt(dealio_v):>14} {fmt(ceo):>15} {fmt(formula):>14} {fmt(diff_ceo):>12} {fmt(diff_formula):>14}")

print("-" * 90)
print(f"{'TOTAL':<12} {fmt(dealio_total):>14} {fmt(ceo_total):>15} {fmt(credit_total):>14} {fmt(ceo_total-dealio_total):>12} {fmt(credit_total-dealio_total):>14}")
