"""
debug_netdep_source.py

Compares net deposits from three sources for each past date:
  1. mv_daily_kpis.net_usd          (our internal source, used in live formula)
  2. SUM(convertednetdeposit) from dealio_daily_profits  (equity_logins only)
  3. SUM(convertednetdeposit) from dealio.daily_profits  (all logins, replica)

If these differ, Dealio's dashboard is likely using a different net_deposit figure
than what we subtract — which directly causes the daily_pnl_cash gap.

Run:
    docker exec reporting-system-app-1 python debug_netdep_source.py
"""

from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
from datetime import date, timedelta

KNOWN_DEALIO_PNL = {
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

DATES = sorted(KNOWN_DEALIO_PNL.keys(), reverse=True)

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

    # mv_daily_kpis net_usd per date
    cur.execute("""
        SELECT tx_date, COALESCE(SUM(net_usd), 0)
        FROM mv_daily_kpis
        WHERE tx_date BETWEEN '2026-03-14' AND '2026-03-23'
        GROUP BY tx_date
    """)
    kpis_nd = {r[0]: float(r[1]) for r in cur.fetchall()}

    # dealio_daily_profits convertednetdeposit (our PG, equity_logins)
    cur.execute("""
        SELECT date::date, COALESCE(SUM(d.convertednetdeposit), 0)
        FROM (
            SELECT DISTINCT ON (login) login, date::date AS date, convertednetdeposit
            FROM dealio_daily_profits
            WHERE date::date BETWEEN '2026-03-14' AND '2026-03-23'
              AND login = ANY(%s)
            ORDER BY login, date DESC
        ) d
        GROUP BY date::date
    """, (equity_logins,))
    pg_nd_filtered = {r[0]: float(r[1]) for r in cur.fetchall()}

pg.close()

dc = get_dealio_connection()
with dc.cursor() as cur:
    # dealio.daily_profits convertednetdeposit (replica, all logins)
    cur.execute("""
        SELECT date::date, COALESCE(SUM(d.convertednetdeposit), 0)
        FROM (
            SELECT DISTINCT ON (login) login, date::date AS date, convertednetdeposit
            FROM dealio.daily_profits
            WHERE date::date BETWEEN '2026-03-14' AND '2026-03-23'
            ORDER BY login, date DESC
        ) d
        GROUP BY date::date
    """)
    dc_nd_all = {r[0]: float(r[1]) for r in cur.fetchall()}
dc.close()

print(f"{'Date':<12} {'mv_kpis':>12} {'PG filtered':>12} {'DC all logins':>14}  {'kpis vs DC':>12}")
print("─" * 70)
for d in DATES:
    kpis  = kpis_nd.get(d, 0)
    pg_nd = pg_nd_filtered.get(d, 0)
    dc_nd = dc_nd_all.get(d, 0)
    diff  = kpis - dc_nd
    print(f"{str(d):<12} {kpis:>12,.0f} {pg_nd:>12,.0f} {dc_nd:>14,.0f}  {diff:>+12,.0f}")

print()
print("kpis = mv_daily_kpis.net_usd (our source)")
print("PG filtered = dealio_daily_profits filtered to equity_logins")
print("DC all logins = dealio.daily_profits all logins (replica)")
print()

# Now: what if we use DC all logins net_dep in F6?
# F6 already uses DC convertednetdeposit. The gap to Dealio is what we saw before.
# This output will show if the net_dep sources differ, which explains the F6 gap.
print("For reference — Dealio known daily_pnl_cash:")
for d in DATES:
    print(f"  {d}: {KNOWN_DEALIO_PNL[d]:>10,}")
