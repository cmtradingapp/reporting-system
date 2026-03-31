"""
Compare yesterday's EEZ snapshot vs what our Method B formula gives for yesterday.
Uses dealio_daily_profits (EOD values) to reconstruct Method B for yesterday.
"""
import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today     = datetime.now(_TZ).date()
yesterday = today - timedelta(days=1)
d_yest    = str(yesterday)
print(f"Comparing for date: {d_yest}\n")

conn = get_connection()
try:
    with conn.cursor() as cur:

        # ── 1. Ground truth: yesterday's snapshot ─────────────────────────
        cur.execute("""
            SELECT COALESCE(SUM(end_equity_zeroed), 0)
            FROM daily_equity_zeroed
            WHERE day = %s::date
              AND login IN (
                  SELECT login::bigint FROM trading_accounts
                  WHERE vtigeraccountid IS NOT NULL
                    AND (deleted = 0 OR deleted IS NULL)
              )
        """, (d_yest,))
        snapshot = float(cur.fetchone()[0] or 0)

        # ── 2. Method B equivalent using dealio_daily_profits for yesterday ─
        # convertedbalance + convertedfloatingpnl = EOD equity (USD)
        # Deduct: compcredit is not in daily_profits, so we check both:
        #   (a) no credit deduction  — matches historical_calc
        #   (b) with credit from local dealio_users snapshot (best proxy)

        # 2a. Bonus map up to yesterday
        cur.execute("""
            SELECT login, SUM(net_amount)
            FROM bonus_transactions
            WHERE confirmation_time < %s::date + INTERVAL '1 day'
            GROUP BY login
        """, (d_yest,))
        bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # 2b. Test account flags
        cur.execute("""
            SELECT ta.login::bigint, MAX(a.is_test_account)
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
            GROUP BY ta.login::bigint
        """)
        test_map = {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}

        # 2c. Credit map from local warehouse (latest available)
        cur.execute("SELECT login::bigint, credit FROM trading_accounts WHERE credit > 0")
        credit_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # 2d. Latest EOD equity per login up to yesterday from dealio_daily_profits
        cur.execute("""
            SELECT DISTINCT ON (login)
                login, convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date <= %s::date
            ORDER BY login, date DESC
        """, (d_yest,))
        dp_rows = cur.fetchall()

        # ── 3. daily_equity_zeroed per login for yesterday (for per-login diff) ──
        cur.execute("""
            SELECT login, end_equity_zeroed
            FROM daily_equity_zeroed
            WHERE day = %s::date
        """, (d_yest,))
        snap_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

finally:
    conn.close()

# ── Calculate ──────────────────────────────────────────────────────────────────
method_b_no_credit  = 0.0
method_b_with_credit = 0.0
n_accounts = 0

for login, bal, flt in dp_rows:
    login = int(login)
    if test_map.get(login, 0) == 1:
        continue
    n_accounts += 1
    eq    = float(bal or 0) + float(flt or 0)
    bonus = max(0.0, bonus_map.get(login, 0.0))
    cr    = credit_map.get(login, 0.0)

    method_b_no_credit   += max(0.0, eq - bonus)
    method_b_with_credit += max(0.0, eq - cr - bonus)

# ── Output ─────────────────────────────────────────────────────────────────────
print(f"{'─'*65}")
print(f"  Accounts in dealio_daily_profits (non-test): {n_accounts:>12,}")
print(f"{'─'*65}")
print(f"  Snapshot  (daily_equity_zeroed yesterday):   {snapshot:>14,.0f}")
print(f"  Method B  (bal+flt - bonus, no credit):      {method_b_no_credit:>14,.0f}   diff={method_b_no_credit-snapshot:>+12,.0f}")
print(f"  Method B  (bal+flt - credit - bonus):        {method_b_with_credit:>14,.0f}   diff={method_b_with_credit-snapshot:>+12,.0f}")
print(f"{'─'*65}")
print(f"\n  Today Live EEZ (Method B, currently shown):  1,156,919")
print(f"  CEO formula result:                          ~2,056,000  (approx)")
