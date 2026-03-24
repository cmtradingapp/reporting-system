from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app.db.postgres_conn import get_connection
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today = str(datetime.now(_TZ).date())

pg = get_connection()
with pg.cursor() as cur:
    cur.execute('''
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0 AND (ta.deleted=0 OR ta.deleted IS NULL) AND a.is_test_account=0
    ''')
    equity_logins = [r[0] for r in cur.fetchall()]
pg.close()

# Step 1: get current floating + open_logins from dealio.positions
dc = get_dealio_connection()
with dc.cursor() as cur:
    cur.execute('''
        SELECT login,
               SUM(COALESCE(computedcommission,0)+COALESCE(computedprofit,0)+COALESCE(computedswap,0))
        FROM dealio.positions
        WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
        GROUP BY login
    ''', (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
    floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
    open_logins = list(floating_map.keys())

    cur.execute('''
        SELECT COALESCE(SUM(COALESCE(computed_commission,0)+COALESCE(computed_profit,0)+COALESCE(computed_swap,0)),0)
        FROM dealio.trades_mt4
        WHERE login = ANY(%s)
          AND close_time >= %s::date
          AND close_time <  %s::date + INTERVAL '1 day'
          AND cmd < 2
          AND symbol NOT IN %s
    ''', (equity_logins, today, today, _EXCLUDED_SYMBOLS_TUPLE))
    today_closed = float(cur.fetchone()[0])
dc.close()

# Step 2: eod_floating_yesterday — only for open_logins (same as _live_calc)
pg2 = get_connection()
with pg2.cursor() as cur:
    cur.execute('''
        SELECT COALESCE(SUM(COALESCE(d.convertedfloatingpnl,0)),0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s::date - INTERVAL '1 day'
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
    ''', (today, open_logins))
    eod_floating_yesterday = float(cur.fetchone()[0])
pg2.close()

current_floating = sum(floating_map.values())
delta = current_floating - eod_floating_yesterday
daily_pnl = round(delta + today_closed)

print(f'today (Cyprus):         {today}')
print(f'equity_logins:          {len(equity_logins):,}')
print(f'open_logins:            {len(open_logins):,}')
print(f'current_floating:       ${current_floating:,.0f}')
print(f'eod_floating_yesterday: ${eod_floating_yesterday:,.0f}')
print(f'delta_floating:         ${delta:,.0f}')
print(f'today_closed_pnl:       ${today_closed:,.0f}')
print(f'DAILY PNL:              ${daily_pnl:,.0f}')
