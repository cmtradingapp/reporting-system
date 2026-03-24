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

    # For each date, compute all formula variations
    header = f"{'Date':<12} {'Dealio':>10} {'F1':>10} {'F2':>10} {'F3':>10} {'F4':>10} {'F5':>10}"
    print(header)
    print("─" * len(header))

    for d in sorted(KNOWN.keys(), reverse=True):
        d_prev = d - timedelta(days=1)

        # EOD values for date d (from dealio.daily_profits)
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0) AS eod_equity_clipped,
                COALESCE(SUM(COALESCE(convertedequity,0)), 0)                AS eod_conv_equity,
                COALESCE(SUM(COALESCE(convertednetdeposit,0)), 0)            AS net_dep
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0)  AS cb,
                    COALESCE(convertedfloatingpnl, 0)  AS cf,
                    convertedequity,
                    convertednetdeposit
                FROM dealio.daily_profits
                WHERE date::date = %s
                  AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d), equity_logins))
        row = cur.fetchone()
        eod_clipped   = float(row[0])
        eod_conv_eq   = float(row[1])
        net_dep       = float(row[2])

        # SOD option A: yesterday's convertedbalance + convertedfloatingpnl (our current)
        cur.execute("""
            SELECT COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s
                  AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev), equity_logins))
        sod_yesterday = float(cur.fetchone()[0])

        # SOD option B: equityprevday from today's records
        cur.execute("""
            SELECT COALESCE(SUM(COALESCE(equityprevday,0)), 0),
                   COALESCE(SUM(CASE WHEN equityprevday > 0 THEN equityprevday ELSE 0 END), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    equityprevday
                FROM dealio.daily_profits
                WHERE date::date = %s
                  AND login = ANY(%s)
                ORDER BY login, date DESC
            ) x
        """, (str(d), equity_logins))
        row = cur.fetchone()
        sod_epd_raw     = float(row[0])
        sod_epd_clipped = float(row[1])

        bonus = bonus_by_date.get(d, 0.0)

        f1 = round(eod_clipped - sod_yesterday   - net_dep)           # our current
        f2 = round(eod_clipped - sod_epd_clipped - net_dep)           # equityprevday clipped
        f3 = round(eod_clipped - sod_yesterday   - net_dep - bonus)   # our current + bonus
        f4 = round(eod_clipped - sod_epd_raw     - net_dep)           # equityprevday unclipped
        f5 = round(eod_conv_eq - sod_epd_raw     - net_dep)           # convertedequity vs epd

        known = KNOWN[d]
        print(f"{str(d):<12} {known:>10,} {f1:>10,} {f2:>10,} {f3:>10,} {f4:>10,} {f5:>10,}")

    print()
    print("F1 = MAX(0,cb+cf) EOD  - MAX(0,cb+cf) SOD yesterday  - net_dep          [current]")
    print("F2 = MAX(0,cb+cf) EOD  - MAX(0,equityprevday)         - net_dep")
    print("F3 = F1 - bonuses")
    print("F4 = MAX(0,cb+cf) EOD  - equityprevday (no clip)      - net_dep")
    print("F5 = convertedequity   - equityprevday (no clip)      - net_dep")

dc.close()
