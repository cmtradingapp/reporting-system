"""
match_dealio_formula.py

Uses dealio.daily_profits directly from the Dealio replica to test formula variations
and find which one matches Dealio's known reported daily_pnl_cash values.

Known Dealio values:
  2026-03-23: -285,438
  2026-03-22:  -14,425
  2026-03-21:  -44,155
  2026-03-20: -270,655
  2026-03-19: -422,665
  2026-03-18: -654,361
  2026-03-17: -175,504
  2026-03-16: -191,888
  2026-03-15:   -1,527
  2026-03-14:  -57,923

Formulas tested (all from dealio.daily_profits directly):
  F1: SUM(MAX(0, cb+cf)) EOD - SUM(MAX(0, cb_prev+cf_prev)) SOD - net_dep         (our current)
  F2: SUM(MAX(0, cb+cf)) EOD - SUM(MAX(0, equityprevday))   SOD - net_dep         (use equityprevday as SOD)
  F3: SUM(MAX(0, cb+cf)) EOD - SUM(MAX(0, cb_prev+cf_prev)) SOD - net_dep - bonus (our current + bonus)
  F4: SUM(MAX(0, cb+cf)) EOD - SUM(equityprevday)           SOD - net_dep         (no clip on SOD)
  F5: SUM(MAX(0, convertedequity)) EOD - SUM(MAX(0, equityprevday)) SOD - net_dep (use convertedequity)

Run:
    docker exec reporting-system-app-1 python match_dealio_formula.py
"""

from app.db.dealio_conn import get_dealio_connection
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

# equity_logins from our DB
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

    # Bonuses per date
    bonus_by_date = {}
    cur.execute("""
        SELECT confirmation_time::date, COALESCE(SUM(net_amount), 0)
        FROM bonus_transactions
        WHERE confirmation_time::date >= '2026-03-14'
          AND confirmation_time::date <= '2026-03-23'
        GROUP BY confirmation_time::date
    """)
    for row in cur.fetchall():
        bonus_by_date[row[0]] = float(row[1])
pg.close()

print(f"equity_logins: {len(equity_logins):,}")
print()

dc = get_dealio_connection()
with dc.cursor() as cur:

    # Check what's available in dealio.daily_profits for past dates
    cur.execute("""
        SELECT date::date, COUNT(*)
        FROM dealio.daily_profits
        WHERE date::date BETWEEN '2026-03-13' AND '2026-03-23'
        GROUP BY date::date
        ORDER BY date::date
    """)
    rows = cur.fetchall()
    print("Records in dealio.daily_profits by date:")
    for r in rows:
        print(f"  {r[0]}: {r[1]:,} rows")
    print()

    # For each date, compute formula variations with and without login filter
    header = f"{'Date':<12} {'Dealio':>10} {'F1 filtered':>12} {'F6 all logins':>14} {'gap F1':>10} {'gap F6':>10}"
    print(header)
    print("─" * len(header))

    for d in sorted(KNOWN.keys(), reverse=True):
        d_prev = d - timedelta(days=1)

        # F1: our equity_logins filter
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0),
                COALESCE(SUM(COALESCE(convertednetdeposit,0)), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf,
                    convertednetdeposit
                FROM dealio.daily_profits
                WHERE date::date = %s AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d), equity_logins))
        row = cur.fetchone()
        eod_f = float(row[0]); net_dep_f = float(row[1])

        cur.execute("""
            SELECT COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev), equity_logins))
        sod_f = float(cur.fetchone()[0])

        # F6: ALL logins in dealio.daily_profits (no filter)
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0),
                COALESCE(SUM(COALESCE(convertednetdeposit,0)), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf,
                    convertednetdeposit
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        row = cur.fetchone()
        eod_a = float(row[0]); net_dep_a = float(row[1])

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
        sod_a = float(cur.fetchone()[0])

        f1 = round(eod_f - sod_f - net_dep_f)
        f6 = round(eod_a - sod_a - net_dep_a)
        known = KNOWN[d]

        print(f"{str(d):<12} {known:>10,} {f1:>12,} {f6:>14,} {known-f1:>+10,} {known-f6:>+10,}")

    print()
    print("F1 = MAX(0,cb+cf) EOD - MAX(0,cb+cf) SOD - net_dep  [equity_logins only]")
    print("F6 = MAX(0,cb+cf) EOD - MAX(0,cb+cf) SOD - net_dep  [ALL logins in daily_profits]")

dc.close()
