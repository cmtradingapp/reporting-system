"""Debug daily PnL components — uses open_logins for eod_floating (matches fix)."""
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
conn.close()

print("\nQuerying live dealio for current floating PnL...")
open_pnl_rows    = get_dealio_floating_pnl_for_logins(equity_logins)
current_floating = sum(float(r[1] or 0) for r in open_pnl_rows)
open_logins      = [int(r[0]) for r in open_pnl_rows]
print(f"current_floating: ${current_floating:,.2f}  (logins with open PnL: {len(open_logins)})")

print("\nQuerying live dealio for today closed PnL...")
closed_rows      = get_dealio_closed_pnl_for_logins_date(equity_logins, str(D))
today_closed_pnl = sum(float(r[1] or 0) for r in closed_rows)
print(f"today_closed_pnl: ${today_closed_pnl:,.2f}  (logins with closed trades today: {len(closed_rows)})")

print(f"\nQuerying eod_floating_yesterday for {len(open_logins)} open_logins only...")
conn2 = get_connection()
with conn2.cursor() as cur:
    cur.execute("""
        SELECT COALESCE(SUM(COALESCE(d.convertedfloatingpnl, 0)), 0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d WHERE d.login = ANY(%s)
    """, (str(PREV), open_logins))
    eod_floating_yesterday = float(cur.fetchone()[0] or 0)
conn2.close()
print(f"eod_floating_yesterday (open_logins only): ${eod_floating_yesterday:,.2f}")

delta_floating = current_floating - eod_floating_yesterday
print(f"\ndelta_floating:  ${delta_floating:,.2f}")
print(f"today_closed:    ${today_closed_pnl:,.2f}")
print(f"daily_pnl:       ${delta_floating + today_closed_pnl:,.2f}")
