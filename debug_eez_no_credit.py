"""
Test Method B WITHOUT compcredit deduction — aligning with the snapshot formula.
Snapshot: MAX(0, convertedbalance + convertedfloatingpnl - bonus)
Method B corrected: MAX(0, compbalance + live_floating - bonus)
"""
import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today = datetime.now(_TZ).date()
d = str(today)
print(f"Date: {d}\n")

conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ta.login::bigint
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE ta.equity > 0
              AND (ta.deleted = 0 OR ta.deleted IS NULL)
              AND a.is_test_account = 0
        """)
        equity_logins = [int(r[0]) for r in cur.fetchall()]

        cur.execute("""
            SELECT login, SUM(net_amount)
            FROM bonus_transactions
            WHERE confirmation_time < %s::date + INTERVAL '1 day'
              AND login = ANY(%s)
            GROUP BY login
        """, (d, equity_logins))
        bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        cur.execute("""
            SELECT COALESCE(SUM(end_equity_zeroed), 0)
            FROM daily_equity_zeroed
            WHERE day = %s::date - INTERVAL '1 day'
              AND login IN (
                  SELECT login::bigint FROM trading_accounts
                  WHERE vtigeraccountid IS NOT NULL
                    AND (deleted = 0 OR deleted IS NULL)
              )
        """, (d,))
        start_eez = float(cur.fetchone()[0] or 0)
finally:
    conn.close()

import time

def _dealio_fetch(equity_logins):
    dc = get_dealio_connection()
    try:
        with dc.cursor() as cur:
            cur.execute("""
                SELECT login, compbalance, compcredit
                FROM dealio.users WHERE login = ANY(%s)
            """, (equity_logins,))
            bal_rows = cur.fetchall()

            cur.execute("""
                SELECT login,
                       SUM(COALESCE(computedcommission,0)
                         + COALESCE(computedprofit,0)
                         + COALESCE(computedswap,0))
                FROM dealio.positions
                WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
                GROUP BY login
            """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
            floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
        return bal_rows, floating_map
    finally:
        dc.close()

for attempt in range(4):
    try:
        bal_rows, floating_map = _dealio_fetch(equity_logins)
        break
    except Exception as e:
        if attempt < 3 and any(s in str(e).lower() for s in ("conflict with recovery", "ssl syscall", "eof")):
            print(f"  Retry {attempt+1} after dealio error: {e}")
            time.sleep(2)
        else:
            raise

total_with_credit    = 0.0
total_without_credit = 0.0
total_credit         = 0.0

for login, bal, credit in bal_rows:
    login  = int(login)
    flt    = floating_map.get(login, 0.0)
    bal    = float(bal or 0)
    cr     = float(credit or 0)
    bonus  = max(0.0, bonus_map.get(login, 0.0))

    total_with_credit    += max(0.0, bal + flt - cr - bonus)
    total_without_credit += max(0.0, bal + flt - bonus)
    total_credit         += cr

print(f"{'─'*60}")
print(f"  Start EEZ (yesterday snapshot):  {start_eez:>14,.0f}")
print(f"{'─'*60}")
print(f"  Method B (bal+flt - credit - bonus):  {total_with_credit:>12,.0f}  ← current (WRONG)")
print(f"  Method B (bal+flt - bonus only):      {total_without_credit:>12,.0f}  ← corrected")
print(f"{'─'*60}")
print(f"  Total compcredit being deducted:      {total_credit:>12,.0f}")
print(f"  Equity logins: {len(equity_logins):,}  |  With positions: {len(floating_map):,}")
