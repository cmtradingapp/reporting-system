"""
Show top 20 accounts by absolute floating PnL to check if -$51M is realistic.
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
finally:
    conn.close()

dc = get_dealio_connection()
try:
    with dc.cursor() as cur:
        cur.execute("""
            SELECT login,
                   SUM(COALESCE(computedcommission,0)
                     + COALESCE(computedprofit,0)
                     + COALESCE(computedswap,0)) AS floating,
                   COUNT(*) AS open_trades
            FROM dealio.positions
            WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
            GROUP BY login
            ORDER BY ABS(SUM(COALESCE(computedcommission,0)
                           + COALESCE(computedprofit,0)
                           + COALESCE(computedswap,0))) DESC
            LIMIT 20
        """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
        rows = cur.fetchall()

        # Also get compbalance for context
        top_logins = [int(r[0]) for r in rows]
        cur.execute(
            "SELECT login, compbalance, compcredit FROM dealio.users WHERE login = ANY(%s)",
            (top_logins,)
        )
        bal_data = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()}
finally:
    dc.close()

print(f"{'Login':>12} {'Floating':>14} {'Trades':>8} {'CompBal':>14} {'CompCredit':>12} {'NetEq':>14}")
print("─" * 80)
total = 0.0
for r in rows:
    login = int(r[0])
    flt   = float(r[1] or 0)
    n     = int(r[2])
    bal, cr = bal_data.get(login, (0.0, 0.0))
    net_eq = bal + flt - cr
    total += flt
    print(f"{login:>12} {flt:>14,.0f} {n:>8} {bal:>14,.0f} {cr:>12,.0f} {net_eq:>14,.0f}")

print(f"\n  Total floating (top 20): {total:>14,.0f}")
