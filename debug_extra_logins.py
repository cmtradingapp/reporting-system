"""
debug_extra_logins.py

Investigates the ~13.5K logins in dealio.daily_profits that are NOT in our equity_logins.
Shows their groupname, sourcename breakdown and equity contribution to understand
which ones Dealio includes/excludes in their daily_pnl_cash.

Run:
    docker exec reporting-system-app-1 python debug_extra_logins.py
"""

from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
from datetime import date

CHECK_DATE = date(2026, 3, 23)  # use a recent date with known Dealio value: -285,438

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
    equity_logins = set(int(r[0]) for r in cur.fetchall())
pg.close()

print(f"Our equity_logins: {len(equity_logins):,}")
print(f"Check date: {CHECK_DATE}")
print()

dc = get_dealio_connection()
with dc.cursor() as cur:

    # All logins in daily_profits for this date
    cur.execute("""
        SELECT DISTINCT ON (login)
            login,
            COALESCE(convertedbalance, 0) AS cb,
            COALESCE(convertedfloatingpnl, 0) AS cf,
            COALESCE(convertednetdeposit, 0) AS nd,
            groupname,
            sourcename
        FROM dealio.daily_profits
        WHERE date::date = %s
        ORDER BY login, date DESC
    """, (str(CHECK_DATE),))
    all_rows = cur.fetchall()

all_logins_dp = {int(r[0]) for r in all_rows}
extra_logins  = all_logins_dp - equity_logins
our_logins    = all_logins_dp & equity_logins

print(f"Logins in daily_profits ({CHECK_DATE}): {len(all_logins_dp):,}")
print(f"  In our equity_logins:    {len(our_logins):,}")
print(f"  NOT in our equity_logins:{len(extra_logins):,}")
print()

# Break down extra logins by groupname and sourcename
from collections import Counter, defaultdict

extra_rows = [r for r in all_rows if int(r[0]) in extra_logins]

# Equity contribution of extra logins
extra_eod = sum(max(0.0, float(r[1]) + float(r[2])) for r in extra_rows)
extra_nd  = sum(float(r[3]) for r in extra_rows)

# Group breakdown
group_counts  = Counter(r[4] for r in extra_rows)
source_counts = Counter(r[5] for r in extra_rows)

# Equity > 0 vs = 0 breakdown
zero_equity  = sum(1 for r in extra_rows if float(r[1]) + float(r[2]) <= 0)
pos_equity   = sum(1 for r in extra_rows if float(r[1]) + float(r[2]) >  0)
pos_eq_total = sum(max(0.0, float(r[1]) + float(r[2])) for r in extra_rows if float(r[1]) + float(r[2]) > 0)

print(f"Extra logins breakdown:")
print(f"  equity <= 0 (clipped to 0): {zero_equity:,}")
print(f"  equity >  0:                {pos_equity:,}  (sum = ${pos_eq_total:,.0f})")
print(f"  net_deposit contribution:   ${extra_nd:,.0f}")
print()

print("Top 20 groupnames (extra logins):")
for gn, cnt in group_counts.most_common(20):
    print(f"  {cnt:>5,}  {gn}")
print()

print("Sources (extra logins):")
for sn, cnt in source_counts.most_common():
    print(f"  {cnt:>5,}  {sn}")
print()

# Show the equity/SOD impact if we include them
print(f"Impact of including extra logins on F6:")
print(f"  Extra EOD equity (MAX(0,...)):  ${extra_eod:>12,.0f}")
print(f"  Extra net deposits:            ${extra_nd:>12,.0f}")
dc.close()
