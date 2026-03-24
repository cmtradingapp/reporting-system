"""
debug_netdep_filtered.py

Tests net_deposits = trades_mt4 cmd=6 WITH symbol exclusions (same list we exclude
from positions). The excluded symbols (FEE, Cashback, Dividend, Rollover etc.) are
MT4 balance operation types — including them inflates/deflates net_deposits.

Dealio likely uses net_deposits = real cash deposits/withdrawals only (no internal
adjustment symbols).

Run:
    docker exec reporting-system-app-1 python debug_netdep_filtered.py
"""

from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app.db.postgres_conn import get_connection
from datetime import date, timedelta

KNOWN = {
    date(2026,3,23): -285438,
    date(2026,3,22):  -14425,
    date(2026,3,21):  -44155,
    date(2026,3,20): -270655,
    date(2026,3,19): -422665,
    date(2026,3,18): -654361,
    date(2026,3,17): -175504,
    date(2026,3,16): -191888,
    date(2026,3,15):   -1527,
    date(2026,3,14):  -57923,
}

DATES = sorted(KNOWN.keys(), reverse=True)

pg = get_connection()
with pg.cursor() as cur:
    cur.execute("""
        SELECT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]
pg.close()

dc = get_dealio_connection()
with dc.cursor() as cur:

    # First: show breakdown of cmd=6 by symbol for a sample date
    print("cmd=6 breakdown by symbol (23/3, all logins):")
    cur.execute("""
        SELECT symbol, COUNT(*), COALESCE(SUM(computed_profit),0) AS total
        FROM dealio.trades_mt4
        WHERE close_time::date = '2026-03-23' AND cmd = 6
        GROUP BY symbol
        ORDER BY total DESC
    """)
    for r in cur.fetchall():
        sym = r[0] or '(no symbol)'
        marker = " *** EXCLUDED" if r[0] in set(_EXCLUDED_SYMBOLS_TUPLE) else ""
        print(f"  {sym:<20} {r[1]:>5,} ops  {float(r[2]):>12,.0f}{marker}")
    print()

    # For each date: net_dep ALL vs net_dep CASH ONLY (symbol not in excluded)
    print(f"{'Date':<12} {'nd_all':>10} {'nd_cash':>10} {'diff':>8}  {'Dealio':>10}  {'F6+nd_all':>10}  {'F6+nd_cash':>10}  gap_all  gap_cash")
    print("─" * 100)

    for d in DATES:
        d_prev = d - timedelta(days=1)

        # EOD all logins
        cur.execute("""
            SELECT COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance, 0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        eod = float(cur.fetchone()[0])

        # SOD all logins
        cur.execute("""
            SELECT COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance, 0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev),))
        sod = float(cur.fetchone()[0])

        eod_minus_sod = eod - sod

        # net_dep ALL cmd=6
        cur.execute("""
            SELECT COALESCE(SUM(COALESCE(computed_profit,0)), 0)
            FROM dealio.trades_mt4
            WHERE close_time::date = %s AND cmd = 6
        """, (str(d),))
        nd_all = float(cur.fetchone()[0])

        # net_dep CASH ONLY (exclude adjustment symbols)
        cur.execute("""
            SELECT COALESCE(SUM(COALESCE(computed_profit,0)), 0)
            FROM dealio.trades_mt4
            WHERE close_time::date = %s
              AND cmd = 6
              AND (symbol IS NULL OR symbol NOT IN %s OR symbol = '')
        """, (str(d), _EXCLUDED_SYMBOLS_TUPLE))
        nd_cash = float(cur.fetchone()[0])

        f6_all  = round(eod_minus_sod - nd_all)
        f6_cash = round(eod_minus_sod - nd_cash)
        known   = KNOWN[d]

        print(f"{str(d):<12} {nd_all:>10,.0f} {nd_cash:>10,.0f} {nd_cash-nd_all:>+8,.0f}  {known:>10,}  {f6_all:>10,}  {f6_cash:>10,}  {known-f6_all:>+7,}  {known-f6_cash:>+8,}")

dc.close()
