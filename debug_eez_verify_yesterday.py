"""
Verify our current EEZ formula against yesterday's confirmed snapshot.

Current formula: MAX(0, convertedbalance + convertedfloatingpnl - bonus)
Using exact date match (= yesterday) — same as run_daily_equity_zeroed_snapshot.

If our formula is correct, the result should match daily_equity_zeroed exactly.
"""
import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today     = datetime.now(_TZ).date()
yesterday = today - timedelta(days=1)
d = str(yesterday)
print(f"Checking formula for: {d}\n")

conn = get_connection()
try:
    with conn.cursor() as cur:

        # 1. Stored snapshot (ground truth)
        cur.execute("""
            SELECT COALESCE(SUM(end_equity_zeroed), 0)
            FROM daily_equity_zeroed
            WHERE day = %s::date
              AND login IN (
                  SELECT login::bigint FROM trading_accounts
                  WHERE vtigeraccountid IS NOT NULL
                    AND (deleted = 0 OR deleted IS NULL)
              )
        """, (d,))
        snapshot = float(cur.fetchone()[0] or 0)

        # 2. Replay our formula with exact date match
        cur.execute("""
            WITH latest_equity AS (
                SELECT DISTINCT ON (login)
                    login, convertedbalance, convertedfloatingpnl
                FROM dealio_daily_profits
                WHERE date::date = %s::date
                ORDER BY login, date DESC
            ),
            bonus_bal AS (
                SELECT login, SUM(net_amount) AS bonus
                FROM bonus_transactions
                WHERE confirmation_time < %s::date + INTERVAL '1 day'
                GROUP BY login
            ),
            test_flags AS (
                SELECT ta.login::bigint AS login, MAX(a.is_test_account) AS is_test
                FROM trading_accounts ta
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
                GROUP BY ta.login::bigint
            )
            SELECT
                COALESCE(SUM(
                    GREATEST(
                        COALESCE(le.convertedbalance, 0) + COALESCE(le.convertedfloatingpnl, 0)
                            - GREATEST(0, COALESCE(b.bonus, 0)),
                        0
                    )
                ), 0) AS formula_result,
                COUNT(*) AS accounts
            FROM latest_equity le
            LEFT JOIN bonus_bal b ON b.login = le.login
            JOIN test_flags tf ON tf.login = le.login
            WHERE tf.is_test = 0
              AND COALESCE(le.convertedbalance, 0) + COALESCE(le.convertedfloatingpnl, 0) > 0
        """, (d, d))
        row = cur.fetchone()
        formula_result = float(row[0] or 0)
        n_accounts = int(row[1] or 0)

        # 3. How many logins in snapshot vs formula
        cur.execute("SELECT COUNT(*) FROM daily_equity_zeroed WHERE day = %s::date AND end_equity_zeroed > 0", (d,))
        snap_count = int(cur.fetchone()[0] or 0)

finally:
    conn.close()

diff = formula_result - snapshot
print(f"{'─'*60}")
print(f"  Snapshot (daily_equity_zeroed):  {snapshot:>14,.0f}")
print(f"  Formula replay (exact date):     {formula_result:>14,.0f}")
print(f"  Difference:                      {diff:>+14,.0f}")
print(f"{'─'*60}")
print(f"  Accounts in formula:             {n_accounts:>14,}")
print(f"  Accounts in snapshot (eez > 0):  {snap_count:>14,}")
print(f"{'─'*60}")
if abs(diff) < 100:
    print("  RESULT: Formula matches snapshot ✓")
else:
    print(f"  RESULT: Gap of {diff:,.0f} — investigate further")
