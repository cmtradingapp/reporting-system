"""Debug daily PnL components."""
import sys
sys.path.insert(0, '/app')

from datetime import date
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_floating_pnl_for_logins, get_dealio_closed_pnl_for_logins_date

D = date(2026, 3, 20)
PREV = date(2026, 3, 19)

conn = get_connection()
with conn.cursor() as cur:
    cur.execute("""
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0 AND (ta.deleted=0 OR ta.deleted IS NULL) AND a.is_test_account=0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]
    print(f"equity_logins count: {len(equity_logins)}")

    cur.execute("""
        SELECT COALESCE(SUM(COALESCE(d.convertedfloatingpnl,0)),0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d WHERE d.login = ANY(%s)
    """, (str(PREV), equity_logins))
    eod_float_yesterday = float(cur.fetchone()[0] or 0)
    print(f"eod_floating_yesterday: ${eod_float_yesterday:,.2f}")

    cur.execute("""
        SELECT COUNT(*) FROM dealio_daily_profits WHERE date::date = %s
    """, (str(PREV),))
    print(f"dealio_daily_profits rows for {PREV}: {cur.fetchone()[0]}")

conn.close()

print("\nQuerying live dealio for current floating PnL...")
rows = get_dealio_floating_pnl_for_logins(equity_logins)
current_floating = sum(float(r[1] or 0) for r in rows)
print(f"current_floating: ${current_floating:,.2f}  (logins with open PnL: {len(rows)})")

print("\nQuerying live dealio for today closed PnL...")
rows2 = get_dealio_closed_pnl_for_logins_date(equity_logins, str(D))
today_closed = sum(float(r[1] or 0) for r in rows2)
print(f"today_closed_pnl: ${today_closed:,.2f}  (logins with closed trades today: {len(rows2)})")

delta = current_floating - eod_float_yesterday
print(f"\ndelta_floating: ${delta:,.2f}")
print(f"daily_pnl:      ${delta + today_closed:,.2f}")
