"""
Preview: what Live EEZ would show if we switch to Method B
  Method B = MAX(0, compbalance + live_floating - compcredit - bonus)
All fields are USD-converted (confirmed).
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

# ── 1. Local postgres ─────────────────────────────────────────────────────────
conn = get_connection()
try:
    with conn.cursor() as cur:
        # equity_logins (ta.equity > 0) — same filter as live_equity.py
        cur.execute("""
            SELECT ta.login::bigint
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE ta.equity > 0
              AND (ta.deleted = 0 OR ta.deleted IS NULL)
              AND a.is_test_account = 0
        """)
        equity_logins = [int(r[0]) for r in cur.fetchall()]
        print(f"Equity logins (ta.equity > 0): {len(equity_logins)}")

        # Cumulative bonus per login
        cur.execute("""
            SELECT login, SUM(net_amount) FROM bonus_transactions
            WHERE confirmation_time < %s::date + INTERVAL '1 day'
              AND login = ANY(%s)
            GROUP BY login
        """, (d, equity_logins))
        bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # Start EEZ total (for comparison)
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
        start_eez_total = float(cur.fetchone()[0] or 0)
finally:
    conn.close()

# ── 2. Dealio live data ───────────────────────────────────────────────────────
dc = get_dealio_connection()
try:
    with dc.cursor() as cur:
        # Method A: compprevequity (current system)
        cur.execute(
            "SELECT login, compprevequity, compcredit FROM dealio.users WHERE login = ANY(%s)",
            (equity_logins,)
        )
        method_a_data = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()}

        # Method B: compbalance (live settled balance, USD-converted)
        cur.execute(
            "SELECT login, compbalance, compcredit FROM dealio.users WHERE login = ANY(%s)",
            (equity_logins,)
        )
        method_b_data = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()}

        # Live floating from positions (USD-converted — confirmed)
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
finally:
    dc.close()

# ── 3. Calculate both methods ─────────────────────────────────────────────────
total_a = 0.0
total_b = 0.0

for login in equity_logins:
    bonus = max(0.0, bonus_map.get(login, 0.0))
    flt   = floating_map.get(login, 0.0)

    # Method A (current)
    eq_a, cr_a = method_a_data.get(login, (0.0, 0.0))
    total_a += max(0.0, eq_a - cr_a - bonus)

    # Method B (proposed)
    bal_b, cr_b = method_b_data.get(login, (0.0, 0.0))
    total_b += max(0.0, bal_b + flt - cr_b - bonus)

# ── 4. Output ─────────────────────────────────────────────────────────────────
print(f"\n{'─'*55}")
print(f"  Start EEZ (yesterday):       {start_eez_total:>14,.0f}")
print(f"{'─'*55}")
print(f"  Method A — current display:  {total_a:>14,.0f}  (compprevequity = yesterday)")
print(f"  Method B — proposed live:    {total_b:>14,.0f}  (compbalance + live floating)")
print(f"  Difference (B - A):          {total_b - total_a:>14,.0f}")
print(f"{'─'*55}")
print(f"\n  Accounts in calculation:     {len(equity_logins):>14,}")
print(f"  Accounts with open positions:{len(floating_map):>14,}")
print(f"  Total live floating:         {sum(floating_map.values()):>14,.0f}")
