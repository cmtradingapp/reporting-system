"""
debug_netdep_trades.py

Tests whether net deposits from dealio.trades_mt4 (cmd=6, balance operations)
matches Dealio's known daily_pnl_cash when used in the F6 formula.

MT4 balance operations (cmd=6) = actual deposits/withdrawals/credits processed in MT4.
This is the most direct source for net_deposits in the Dealio formula.

Run:
    docker exec reporting-system-app-1 python debug_netdep_trades.py
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

    # Check what cmd values exist in trades_mt4
    print("cmd values in dealio.trades_mt4 (sample from last 30 days):")
    cur.execute("""
        SELECT cmd, COUNT(*), MIN(close_time), MAX(close_time)
        FROM dealio.trades_mt4
        WHERE close_time >= NOW() - INTERVAL '30 days'
        GROUP BY cmd
        ORDER BY cmd
    """)
    for r in cur.fetchall():
        print(f"  cmd={r[0]}: {r[1]:,} rows  ({r[2]} – {r[3]})")
    print()

    # Net deposits from trades_mt4 per date (all logins)
    # cmd=6 = balance operation in MT4 (deposits/withdrawals/bonuses)
    print("Net deposits from trades_mt4 (cmd=6, all logins) by date:")
    cur.execute("""
        SELECT close_time::date AS dt,
               COUNT(*),
               COALESCE(SUM(COALESCE(computed_profit,0)), 0) AS net_raw
        FROM dealio.trades_mt4
        WHERE close_time::date BETWEEN '2026-03-14' AND '2026-03-23'
          AND cmd = 6
        GROUP BY close_time::date
        ORDER BY dt DESC
    """)
    trades_nd_all = {}
    for r in cur.fetchall():
        trades_nd_all[r[0]] = float(r[2])
        print(f"  {r[0]}: {r[1]:,} ops  net={r[2]:,.0f}")
    print()

    # Also check cmd=6 filtered to equity_logins
    cur.execute("""
        SELECT close_time::date AS dt,
               COALESCE(SUM(COALESCE(computed_profit,0)), 0) AS net_raw
        FROM dealio.trades_mt4
        WHERE close_time::date BETWEEN '2026-03-14' AND '2026-03-23'
          AND cmd = 6
          AND login = ANY(%s)
        GROUP BY close_time::date
        ORDER BY dt DESC
    """, (equity_logins,))
    trades_nd_filtered = {r[0]: float(r[1]) for r in cur.fetchall()}

    print()
    print("F6 formula with trades_mt4 net_dep vs Dealio known values:")
    print()
    header = f"{'Date':<12} {'Dealio':>10} {'t_nd all':>12} {'t_nd filtered':>14} {'F6+t_nd_all':>12} {'F6+t_nd_filt':>13}  gap_all  gap_filt"
    print(header)
    print("─" * len(header))

    for d in DATES:
        d_prev = d - timedelta(days=1)

        # EOD all logins
        cur.execute("""
            SELECT COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        eod = float(cur.fetchone()[0])

        # SOD all logins (yesterday EOD)
        cur.execute("""
            SELECT COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev),))
        sod = float(cur.fetchone()[0])

        eod_minus_sod = eod - sod
        t_nd_all  = trades_nd_all.get(d, 0)
        t_nd_filt = trades_nd_filtered.get(d, 0)

        f6_tnd_all  = round(eod_minus_sod - t_nd_all)
        f6_tnd_filt = round(eod_minus_sod - t_nd_filt)
        known = KNOWN[d]

        print(f"{str(d):<12} {known:>10,} {t_nd_all:>12,.0f} {t_nd_filt:>14,.0f} {f6_tnd_all:>12,} {f6_tnd_filt:>13,}  {known-f6_tnd_all:>+7,}  {known-f6_tnd_filt:>+8,}")

dc.close()
