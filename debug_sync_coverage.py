"""
debug_sync_coverage.py

Checks if dealio_daily_profits (Postgres) is missing logins vs dealio.daily_profits (replica)
per date. debug_table_sync.py confirmed equal row counts and SUM(convertedbalance), but
it's possible the SAME logins are present in both — this script checks per-login coverage.

Shows:
  - How many logins are in replica but NOT in our PG copy per date
  - The equity contribution of those missing logins (MAX(0, cb+cf))

Run:
    docker exec reporting-system-app-1 python debug_sync_coverage.py
"""

from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
from datetime import date, timedelta

DATES = [
    date(2026,3,23),
    date(2026,3,22),
    date(2026,3,21),
    date(2026,3,20),
    date(2026,3,19),
    date(2026,3,18),
    date(2026,3,17),
    date(2026,3,16),
    date(2026,3,15),
    date(2026,3,14),
]

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

dc = get_dealio_connection()
pg = get_connection()

print(f"{'Date':<12} {'DC logins':>10} {'PG logins':>10} {'missing':>8} {'miss equity':>12} {'DC EOD':>12} {'PG EOD':>12} {'EOD diff':>10}")
print("─" * 100)

with dc.cursor() as dc_cur, pg.cursor() as pg_cur:
    for d in DATES:
        # Get all logins from replica for this date (DISTINCT ON login)
        dc_cur.execute("""
            SELECT login,
                   COALESCE(convertedbalance, 0) AS cb,
                   COALESCE(convertedfloatingpnl, 0) AS cf
            FROM (
                SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        dc_rows = {int(r[0]): (float(r[1]), float(r[2])) for r in dc_cur.fetchall()}

        # Get all logins from our Postgres for this date
        pg_cur.execute("""
            SELECT login,
                   COALESCE(convertedbalance, 0) AS cb,
                   COALESCE(convertedfloatingpnl, 0) AS cf
            FROM (
                SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
                FROM dealio_daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        pg_rows = {int(r[0]): (float(r[1]), float(r[2])) for r in pg_cur.fetchall()}

        dc_logins = set(dc_rows.keys())
        pg_logins = set(pg_rows.keys())
        missing_in_pg = dc_logins - pg_logins
        extra_in_pg   = pg_logins - dc_logins

        # Equity contribution of missing logins
        miss_equity = sum(max(0.0, dc_rows[l][0] + dc_rows[l][1]) for l in missing_in_pg)

        # Total EOD equity (MAX(0, cb+cf)) for DC and PG
        dc_eod = sum(max(0.0, cb + cf) for cb, cf in dc_rows.values())
        pg_eod = sum(max(0.0, cb + cf) for cb, cf in pg_rows.values())

        print(f"{str(d):<12} {len(dc_logins):>10,} {len(pg_logins):>10,} {len(missing_in_pg):>8,} {miss_equity:>12,.0f} {dc_eod:>12,.0f} {pg_eod:>12,.0f} {dc_eod-pg_eod:>+10,.0f}")

        if missing_in_pg:
            # Show top 5 missing logins by equity contribution
            top_missing = sorted(missing_in_pg, key=lambda l: max(0.0, dc_rows[l][0]+dc_rows[l][1]), reverse=True)[:5]
            for l in top_missing:
                cb, cf = dc_rows[l]
                print(f"  -> missing login {l}: cb={cb:,.0f}  cf={cf:,.0f}  eq={max(0,cb+cf):,.0f}")

        if extra_in_pg:
            print(f"  !! extra in PG (not in DC): {len(extra_in_pg)} logins")

print()
print("miss equity = SUM(MAX(0, cb+cf)) for logins in DC but NOT in PG")
print("EOD diff    = DC total - PG total (positive = DC has more equity)")

dc.close()
pg.close()
