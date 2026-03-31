"""
Verify dealio_daily_profits timestamp semantics.

If date='2026-03-19 00:00:00' represents EOD March 19:
  convertedfloatingpnl(mar19) - converteddeltafloatingpnl(mar19)
  should approximately equal convertedfloatingpnl(mar18)

If date='2026-03-19 00:00:00' represents EOD March 18:
  convertedfloatingpnl(mar19) + converteddeltafloatingpnl(mar20)
  would equal convertedfloatingpnl(mar20) — but mar20 has no rows yet
"""
import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection

conn = get_connection()
with conn.cursor() as cur:
    # Get total convertedfloatingpnl per day (March 17-19)
    cur.execute("""
        SELECT date::date AS day,
               COUNT(*) AS rows,
               COUNT(DISTINCT login) AS logins,
               SUM(convertedfloatingpnl) AS total_floating,
               SUM(converteddeltafloatingpnl) AS total_delta,
               SUM(convertedclosedpnl) AS total_closed
        FROM dealio_daily_profits
        WHERE date::date BETWEEN '2026-03-17' AND '2026-03-20'
        GROUP BY date::date
        ORDER BY day
    """)
    rows = cur.fetchall()
    print("=== Per-day totals (all logins) ===")
    print(f"{'Day':<12} {'Rows':>8} {'Logins':>8} {'Floating':>15} {'Delta':>15} {'Closed':>15}")
    for r in rows:
        day, cnt, logins, floating, delta, closed = r
        print(f"{str(day):<12} {cnt:>8,} {logins:>8,} {float(floating or 0):>15,.2f} {float(delta or 0):>15,.2f} {float(closed or 0):>15,.2f}")

    # Check: does floating(N) - delta(N) ≈ floating(N-1)?
    print("\n=== Verification: floating(N) - delta(N) should = floating(N-1) if date=EOD(N) ===")
    cur.execute("""
        WITH per_day AS (
            SELECT date::date AS day,
                   SUM(convertedfloatingpnl) AS floating,
                   SUM(converteddeltafloatingpnl) AS delta
            FROM dealio_daily_profits
            WHERE date::date BETWEEN '2026-03-17' AND '2026-03-20'
            GROUP BY date::date
        )
        SELECT a.day,
               a.floating AS floating_a,
               a.delta    AS delta_a,
               b.floating AS floating_prev,
               a.floating - a.delta AS implied_prev,
               ABS((a.floating - a.delta) - COALESCE(b.floating, 0)) AS diff
        FROM per_day a
        LEFT JOIN per_day b ON b.day = a.day - INTERVAL '1 day'
        ORDER BY a.day
    """)
    rows2 = cur.fetchall()
    print(f"{'Day':<12} {'Floating':>15} {'Delta':>15} {'Prev Floating':>15} {'Implied Prev':>15} {'Diff':>12}")
    for r in rows2:
        day, floating, delta, prev_floating, implied_prev, diff = r
        print(f"{str(day):<12} {float(floating or 0):>15,.2f} {float(delta or 0):>15,.2f} {float(prev_floating or 0):>15,.2f} {float(implied_prev or 0):>15,.2f} {float(diff or 0):>12,.2f}")

    # Also check: what time of day do the snapshots arrive?
    print("\n=== Snapshot timestamps (sample of actual date column values) ===")
    cur.execute("""
        SELECT DISTINCT date
        FROM dealio_daily_profits
        WHERE date::date BETWEEN '2026-03-17' AND '2026-03-20'
        ORDER BY date
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"  {r[0]}")

    # Compare live current_floating vs mar19 snapshot
    print("\n=== Live vs snapshot comparison for equity_logins ===")
    cur.execute("""
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0 AND (ta.deleted=0 OR ta.deleted IS NULL) AND a.is_test_account=0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]
    print(f"equity_logins: {len(equity_logins)}")

    for day_label, day_val in [("2026-03-17", "2026-03-17"), ("2026-03-18", "2026-03-18"), ("2026-03-19", "2026-03-19")]:
        cur.execute("""
            SELECT COALESCE(SUM(d.convertedfloatingpnl), 0),
                   COALESCE(SUM(d.converteddeltafloatingpnl), 0)
            FROM (
                SELECT DISTINCT ON (login) login, convertedfloatingpnl, converteddeltafloatingpnl
                FROM dealio_daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) d WHERE d.login = ANY(%s)
        """, (day_val, equity_logins))
        r = cur.fetchone()
        print(f"  {day_label}: floating={float(r[0] or 0):>15,.2f}  delta={float(r[1] or 0):>15,.2f}")

conn.close()
print("\nDone.")
